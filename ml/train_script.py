import argparse
import os
import json
import time
import traceback
import logging
import torch
import mlflow
from datasets import load_dataset
from peft import LoraConfig, prepare_model_for_kbit_training, PeftModel
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
)
from trl import SFTTrainer, SFTConfig

def format_instruction(sample):
    system_prompt = """Ты — система для извлечения параметров напоминаний.
Твоя задача: извлечь текст, дату, время и периодичность из сообщения пользователя и вернуть JSON.
Используй текущую дату (Context Date) для разрешения относительных дат."""
    
    user_content = f"Context Date: {sample['context_date']}\nMessage: \"{sample['input']}\"\n\nJSON:"
    assistant_content = json.dumps(sample['output'], ensure_ascii=False)
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
        {"role": "assistant", "content": assistant_content}
    ]
    return {"messages": messages}

def train(data_path, model_id, output_dir, epochs=1, run_id_file="last_run_id.txt"):
    # Setup logging for better diagnostics
    logger = logging.getLogger("train_script")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    logger.info("Starting training with model %s", model_id)
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("reminder-bot-experiment")
    start_time = time.time()

    with mlflow.start_run() as run:
        run_id = run.info.run_id
        logger.info("MLflow Run ID: %s", run_id)
        
        # Save run_id for subsequent steps
        try:
            with open(run_id_file, "w") as f:
                f.write(run_id)
            logger.info("Saved run_id to %s", run_id_file)
        except Exception as e:
            logger.warning("Could not save run_id to %s: %s", run_id_file, e)
            
        mlflow.log_param("model_id", model_id)
        mlflow.log_param("epochs", epochs)

        # 1. Load Dataset
        logger.debug("Loading dataset from %s", data_path)
        dataset = load_dataset("json", data_files=data_path, split="train")
        dataset = dataset.map(format_instruction)
        logger.info("Dataset loaded: %d samples", len(dataset))

        # 2. Config & Tokenizer
        device_map = "auto"
        model_kwargs = {}

        if torch.cuda.is_available():
            logger.info("GPU detected. Using 4-bit quantization (QLoRA).")
            bnb_config = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_compute_dtype=torch.float16, 
                bnb_4bit_use_double_quant=True,
            )
            model_kwargs["quantization_config"] = bnb_config
            model_kwargs["device_map"] = "auto"
            model_kwargs["torch_dtype"] = torch.float16
        else:
            print("No GPU detected. Using CPU execution (Standard LoRA) with Float32.")
            device_map = {"": "cpu"}
            # On CPU, bitsandbytes 4-bit/8-bit usually requires CUDA.
            # We must use float32 for stable training on CPU (Linear layers crash with Half/Float mismatch)
            model_kwargs["device_map"] = device_map
            model_kwargs["torch_dtype"] = torch.float32

        logger.debug("Loading tokenizer for %s", model_id)
        tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)
        tokenizer.pad_token = tokenizer.eos_token
        logger.info("Tokenizer loaded. Pad token set.")

        # 3. Load Model
        # For CPU: no quantization config, just load model
        model_kwargs["trust_remote_code"] = True
        
        logger.debug("Loading model with kwargs: %s", {k: ('<redacted>' if k=='quantization_config' else str(v)) for k,v in model_kwargs.items()})
        try:
            model = AutoModelForCausalLM.from_pretrained(model_id, **model_kwargs)
            logger.info("Model loaded: %s", model_id)
        except Exception as e:
            logger.exception("Failed to load model %s: %s", model_id, e)
            # Record in MLflow for debugging
            try:
                mlflow.log_param("model_load_error", str(e))
            except Exception:
                logger.debug("Failed to log model_load_error to MLflow")
            raise
        
        # Prepare for kbit training only if quantized
        model.config.use_cache = False
        if torch.cuda.is_available():
            model.gradient_checkpointing_enable()
            model = prepare_model_for_kbit_training(model)
        else:
            # Enable gradients for LoRA adaptation on standard model
            model.enable_input_require_grads()

        # FORCE CAST ANY SUSPICIOUS MODULES TO FLOAT32/FLOAT16
        logger.debug("Explicitly casting LayerNorms and checking for BFloat16...")
        for name, module in model.named_modules():
            if "norm" in name.lower() or "ln" in name.lower():
                try:
                    module.to(torch.float32)
                except Exception:
                    logger.debug("Could not cast module %s to float32", name)
            # Check params
            for p_name, p in module.named_parameters(recurse=False):
                if p.dtype == torch.bfloat16:
                    logger.warning("Found BFloat16 parameter: %s.%s. Casting to Float16.", name, p_name)
                    try:
                        p.data = p.data.to(torch.float16)
                    except Exception:
                        logger.exception("Failed to cast parameter %s.%s", name, p_name)

        # 4. LoRA Config
        peft_config = LoraConfig(
            r=16,
            lora_alpha=32,
            lora_dropout=0.05,
            bias="none",
            task_type="CAUSAL_LM",
            target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"]
        )

        # 5. Training Args
        use_fp16 = False # torch.cuda.is_available() # DISABLE FP16 TO DEBUG
        use_bf16 = False 
        
        training_args = SFTConfig(
            output_dir=output_dir,
            num_train_epochs=epochs,
            per_device_train_batch_size=1 if not torch.cuda.is_available() else 4, # Smaller batch on CPU
            gradient_accumulation_steps=8 if not torch.cuda.is_available() else 4,
            learning_rate=2e-4,
            fp16=use_fp16, # False
            bf16=False, # Safe default for CPU and avoiding errors on some GPUs
            use_cpu=not torch.cuda.is_available(),
            logging_steps=10,
            save_strategy="epoch",
            optim="adamw_torch", # standard adamw works everywhere
            report_to="mlflow",  # Changed from "none" to "mlflow" to enable logging
            max_length=512,
            dataset_text_field="text",
            packing=False
        )

        def safe_formatting_func(example):
            outputs = []
            for msgs in example['messages']:
                try:
                    text = tokenizer.apply_chat_template(msgs, tokenize=False)
                    outputs.append(text)
                except:
                    outputs.append("")
            return outputs

        logger.info("Preparing trainer and PEFT configuration")
        trainer = SFTTrainer(
            model=model,
            train_dataset=dataset,
            args=training_args,
            peft_config=peft_config,
            processing_class=tokenizer,
            formatting_func=safe_formatting_func
        )

        # Run training with robust error handling and diagnostic dumps on failure
        logger.info("Starting trainer.train()")
        train_start = time.time()
        try:
            trainer.train()
            train_dur = time.time() - train_start
            logger.info("trainer.train() completed in %.2f sec", train_dur)
        except Exception as e:
            logger.exception("trainer.train() failed: %s", e)
            # Write traceback to output_dir for offline inspection
            try:
                os.makedirs(output_dir, exist_ok=True)
                tb_path = os.path.join(output_dir, "train_exception.log")
                with open(tb_path, "w") as ef:
                    ef.write(traceback.format_exc())
                logger.info("Traceback written to %s", tb_path)
                try:
                    mlflow.log_artifact(tb_path, artifact_path="errors")
                except Exception:
                    logger.debug("Could not upload traceback to MLflow")
                try:
                    mlflow.log_param("train_error", str(e))
                except Exception:
                    logger.debug("Could not set MLflow param train_error")
            except Exception:
                logger.exception("Could not write traceback to disk")
            raise

        # Save Adapters locally
        adapter_path = os.path.join(output_dir, "final_adapter")
        trainer.model.save_pretrained(adapter_path)
        tokenizer.save_pretrained(adapter_path)
        
        # Log artifacts (adapters) to MLflow/S3
        mlflow.log_artifacts(adapter_path, artifact_path="adapter")
        print("Training complete. Adapters saved and logged to MLflow.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, required=True)
    parser.add_argument("--model_id", type=str, default="Qwen/Qwen2.5-3B-Instruct")
    parser.add_argument("--output_dir", type=str, default="./results")
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--run_id_file", type=str, default="last_run_id.txt")
    args = parser.parse_args()
    
    train(args.data_path, args.model_id, args.output_dir, args.epochs, args.run_id_file)
