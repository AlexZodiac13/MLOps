import argparse
import os
import json
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

def train(data_path, model_id, output_dir, epochs=1):
    print(f"Starting training with model {model_id}")
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("reminder-bot-experiment")
    
    with mlflow.start_run():
        mlflow.log_param("model_id", model_id)
        mlflow.log_param("epochs", epochs)

        # 1. Load Dataset
        dataset = load_dataset("json", data_files=data_path, split="train")
        dataset = dataset.map(format_instruction)
        print(f"Dataset loaded: {len(dataset)} samples")

        # 2. Config & Tokenizer
        device_map = "auto"
        quantization_config = None

        if torch.cuda.is_available():
            print("GPU detected. Using 4-bit quantization (QLoRA).")
            bnb_config = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_compute_dtype=torch.float16, 
                bnb_4bit_use_double_quant=True,
            )
            quantization_config = bnb_config
        else:
            print("No GPU detected. Using CPU execution (Standard LoRA).")
            device_map = {"": "cpu"}
            # On CPU, we don't quantize with bitsandbytes (requires CUDA)
            # We load in float32 or float16 if supported, but float32 is safest for CPU training stability
            
        tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)
        tokenizer.pad_token = tokenizer.eos_token

        # 3. Load Model
        # For CPU: no quantization config, just load model
        model_kwargs = {
            "device_map": device_map,
            "trust_remote_code": True
        }
        if quantization_config:
            model_kwargs["quantization_config"] = quantization_config
            
        model = AutoModelForCausalLM.from_pretrained(model_id, **model_kwargs)
        
        # Prepare for kbit training only if quantized
        model.config.use_cache = False
        if quantization_config:
            model.gradient_checkpointing_enable()
            model = prepare_model_for_kbit_training(model)
        else:
            # Enable gradients for LoRA adaptation on standard model
            model.enable_input_require_grads()

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
        use_fp16 = torch.cuda.is_available() # fp16 on CPU is sometimes slow or unsupported for optim
        use_bf16 = False 
        
        training_args = SFTConfig(
            output_dir=output_dir,
            num_train_epochs=epochs,
            per_device_train_batch_size=1 if not torch.cuda.is_available() else 4, # Smaller batch on CPU
            gradient_accumulation_steps=8 if not torch.cuda.is_available() else 4,
            learning_rate=2e-4,
            fp16=use_fp16, # Use True only if CUDA available
            bf16=False, # Safe default for CPU and avoiding errors on some GPUs
            use_cpu=not torch.cuda.is_available(),
            logging_steps=10,
            save_strategy="epoch",
            optim="adamw_torch", # standard adamw works everywhere
            report_to="none",
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

        trainer = SFTTrainer(
            model=model,
            train_dataset=dataset,
            args=training_args,
            peft_config=peft_config,
            processing_class=tokenizer,
            formatting_func=safe_formatting_func
        )

        trainer.train()

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
    args = parser.parse_args()
    
    train(args.data_path, args.model_id, args.output_dir, args.epochs)
