import os
import json
import torch
import mlflow
import mlflow.transformers
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

# Set storage environment variables for MLflow artifacts
os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY

# Base Model (using a smaller model for CPU demonstration if needed, but keeping 3B as per request)
MODEL_ID = "Qwen/Qwen2.5-3B-Instruct"  # This may require a lot of RAM for training!
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_FILE = os.path.join(SCRIPT_DIR, "labeled_dataset.json")

def format_instruction(sample):
    system_prompt = """Ты — система для извлечения параметров напоминаний.
Твоя задача: извлечь текст, дату, время и периодичность из сообщения пользователя и вернуть JSON."""
    
    user_content = f"Context Date: {sample['context_date']}\nMessage: \"{sample['input']}\"\n\nJSON:"
    assistant_content = json.dumps(sample['output'], ensure_ascii=False)
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
        {"role": "assistant", "content": assistant_content}
    ]
    return {"messages": messages}

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
        dataset = dataset.map(format_instruction)
        
        # 2. Setup Quantization (even on CPU, 4-bit can save memory)
        # Note: bitsandbytes 4-bit usually requires CUDA. For CPU training we might need to skip quantization or use other methods.
        # But per user request we follow the notebook's logic.
        
        tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
        tokenizer.pad_token = tokenizer.eos_token
        
        # Load model logic (CPU fallback)
        device_map = "auto" if torch.cuda.is_available() else "cpu"
        
        model = AutoModelForCausalLM.from_pretrained(
            MODEL_ID,
            # quantization_config=bnb_config if torch.cuda.is_available() else None,
            device_map=device_map,
            trust_remote_code=True
        )

        # 3. Training Arguments (Minimal for proof of concept)
        training_args = SFTConfig(
            output_dir="/tmp/results",
            num_train_epochs=1,
            max_steps=10, # Short for demo
            per_device_train_batch_size=1,
            gradient_accumulation_steps=1,
            learning_rate=2e-4,
            logging_steps=2,
            max_length=512,
            dataset_text_field="text",
            report_to="none" # We manually log to MLflow
        )

        # 4. SFTTrainer
        trainer = SFTTrainer(
            model=model,
            train_dataset=dataset,
            args=training_args,
            processing_class=tokenizer,
            formatting_func=lambda x: [tokenizer.apply_chat_template(m, tokenize=False) for m in x['messages']]
        )

        print("Starting training...")
        train_result = trainer.train()
        
        # 5. Log metrics
        mlflow.log_metric("train_loss", train_result.training_loss)
        print(f"Training loss: {train_result.training_loss}")

        # 6. Merge Adapters for GGUF Export
        print("Merging adapters...")
        from peft import PeftModel
        
        # Reload for merging (in FP16 to save memory)
        base_model = AutoModelForCausalLM.from_pretrained(
            MODEL_ID,
            torch_dtype=torch.float16,
            device_map="cpu", # Force CPU for merging if no GPU
            trust_remote_code=True
        )
        model = PeftModel.from_pretrained(base_model, "/tmp/results")
        merged_model = model.merge_and_unload()
        
        merged_path = "/tmp/model_merged"
        merged_model.save_pretrained(merged_path)
        tokenizer.save_pretrained(merged_path)
        print(f"Merged model saved to {merged_path}")

        # 7. Save to MLflow
        print("Logging merged model to MLflow...")
        mlflow.transformers.log_model(
            transformers_model={"model": merged_model, "tokenizer": tokenizer},
            artifact_path="model_hf"
        )
        
        # Store the local path in a file for the next task (using /tmp/ for cluster compatibility)
        with open("/tmp/merged_path.txt", "w") as f:
            f.write(os.path.abspath(merged_path))
        
        print("Done.")

if __name__ == "__main__":
    train()
