import os
import json
import torch
import mlflow
import mlflow.transformers
import importlib_metadata
from datasets import load_dataset
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
    TrainingArguments
)
from trl import SFTTrainer, SFTConfig
from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training

# Configuration from environment (passed by Airflow)
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_S3_ENDPOINT_URL = os.getenv("MLFLOW_S3_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "ReminderBot_Training")

# Set storage environment variables for MLflow artifacts (only if defined to avoid crash)
if MLFLOW_S3_ENDPOINT_URL:
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
if AWS_ACCESS_KEY_ID:
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
if AWS_SECRET_ACCESS_KEY:
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
if os.getenv("AWS_DEFAULT_REGION"):
     os.environ["AWS_DEFAULT_REGION"] = os.getenv("AWS_DEFAULT_REGION")
if os.getenv("MLFLOW_S3_IGNORE_TLS"):
     os.environ["MLFLOW_S3_IGNORE_TLS"] = os.getenv("MLFLOW_S3_IGNORE_TLS")
if os.getenv("AWS_S3_ADDRESSING_STYLE"):
     os.environ["AWS_S3_ADDRESSING_STYLE"] = os.getenv("AWS_S3_ADDRESSING_STYLE")

# Base Model
# IMPORTANT: For Managed Airflow with c2-m4 workers (4GB RAM), a 3B model will cause OOM (Exit Code -9).
# We use a 0.5B model for the lifecycle demonstration. 
# For 3B model, please increase worker_resource_preset to at least c2-m16 in terraform.
MODEL_ID = os.getenv("MODEL_ID", "Qwen/Qwen2.5-0.5B-Instruct") 
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_FILE = os.path.join(SCRIPT_DIR, "labeled_dataset.json")

def train():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    with mlflow.start_run() as run:
        print(f"Started MLflow run: {run.info.run_id}")
        
        # Log basic info
        mlflow.log_param("model_id", MODEL_ID)
        mlflow.log_param("dataset", DATASET_FILE)

        # 1. Prepare Dataset
        dataset = load_dataset("json", data_files=DATASET_FILE, split="train")
        
        # 2. Setup Tokenizer and Model
        tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
        tokenizer.pad_token = tokenizer.eos_token
        
        # Format messages according to chat template
        def apply_template(sample):
            system_prompt = """Ты — система для извлечения параметров напоминаний.
Твоя задача: извлечь текст, дату, время и периодичность из сообщения пользователя и вернуть JSON."""
            user_content = f"Context Date: {sample['context_date']}\nMessage: \"{sample['input']}\"\n\nJSON:"
            assistant_content = json.dumps(sample['output'], ensure_ascii=False)
            
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
                {"role": "assistant", "content": assistant_content}
            ]
            # Pre-format each example for training
            text = tokenizer.apply_chat_template(messages, tokenize=False)
            return {"text": text}

        print("Formatting dataset...")
        dataset = dataset.map(apply_template)
        
        # Load model logic (CPU fallback)
        device_map = {"": "cpu"}
        
        # Load in float32 for CPU training (float16 is not usually supported for CPU training backends)
        print(f"Loading base model {MODEL_ID}...")
        model = AutoModelForCausalLM.from_pretrained(
            MODEL_ID,
            device_map=device_map,
            trust_remote_code=True,
            torch_dtype=torch.float32
        )

        # 3. LoRA Configuration
        peft_config = LoraConfig(
            r=8,
            lora_alpha=16,
            target_modules=["q_proj", "v_proj"],
            lora_dropout=0.05,
            bias="none",
            task_type="CAUSAL_LM",
        )

        # 4. Training Arguments
        training_args = SFTConfig(
            output_dir="/tmp/results",
            num_train_epochs=1,
            max_steps=10, # Short for demo
            per_device_train_batch_size=1,
            gradient_accumulation_steps=1,
            learning_rate=2e-4,
            logging_steps=2,
            max_length=512,
            dataset_text_field="text", # Points to our new column
            report_to="none", # We manually log to MLflow
            use_cpu=True,
            fp16=False,
            bf16=False
        )

        # 5. SFTTrainer
        trainer = SFTTrainer(
            model=model,
            train_dataset=dataset,
            peft_config=peft_config,
            args=training_args,
            processing_class=tokenizer
        )

        print("Starting training...")
        train_result = trainer.train()
        
        # 6. Log metrics
        mlflow.log_metric("train_loss", train_result.training_loss)
        print(f"Training loss: {train_result.training_loss}")

        # 7. Merge Adapters for GGUF Export
        print("Merging adapters...")
        from peft import PeftModel
        
        # Save lora first
        trainer.model.save_pretrained("/tmp/lora_adapter")
        
        # Reload for merging (float32 for CPU compatibility)
        base_model = AutoModelForCausalLM.from_pretrained(
            MODEL_ID,
            torch_dtype=torch.float32,
            device_map="cpu",
            trust_remote_code=True
        )
        model = PeftModel.from_pretrained(base_model, "/tmp/lora_adapter")
        merged_model = model.merge_and_unload()
        
        merged_path = "/tmp/model_merged"
        merged_model.save_pretrained(merged_path)
        tokenizer.save_pretrained(merged_path)
        print(f"Merged model saved to {merged_path}")

        # 8. Save to MLflow
        print("Logging merged model to MLflow...")
        # Explicitly define requirements to avoid MLflow trying to auto-infer and crashing on missing tensorflow
        pip_requirements = [
            f"torch=={torch.__version__}",
            f"transformers=={importlib_metadata.version('transformers')}",
            f"accelerate=={importlib_metadata.version('accelerate')}",
            f"peft=={importlib_metadata.version('peft')}"
        ]
        
        mlflow.transformers.log_model(
            transformers_model={"model": merged_model, "tokenizer": tokenizer},
            artifact_path="model_hf",
            pip_requirements=pip_requirements
        )
        
        # Store the local path in a file for the next task (using /tmp/ for cluster compatibility)
        with open("/tmp/merged_path.txt", "w") as f:
            f.write(os.path.abspath(merged_path))
        
        print("Done.")

if __name__ == "__main__":
    train()
