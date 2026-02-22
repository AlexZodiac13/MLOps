import argparse
import json
import torch
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer
import mlflow

def test_model(model_id, adapter_path, test_data_path):
    print(f"Loading model for testing: {model_id} + {adapter_path}")
    
    # Load model
    base_model = AutoModelForCausalLM.from_pretrained(
        model_id,
        device_map="auto",
        torch_dtype=torch.float16,
        trust_remote_code=True
    )
    model = PeftModel.from_pretrained(base_model, adapter_path)
    tokenizer = AutoTokenizer.from_pretrained(model_id)

    # Load test data
    with open(test_data_path, 'r') as f:
        data = json.load(f)
    
    # Take a sample
    sample = data[0] if data else None
    if not sample:
        print("No test data found")
        return

    system_prompt = """Ты — система для извлечения параметров напоминаний.
Твоя задача: извлечь текст, дату, время и периодичность из сообщения пользователя и вернуть JSON.
Используй текущую дату (Context Date) для разрешения относительных дат."""
    
    user_content = f"Context Date: {sample['context_date']}\nMessage: \"{sample['input']}\"\n\nJSON:"
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content}
    ]
    
    text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    model_inputs = tokenizer([text], return_tensors="pt").to(model.device)

    print("Generating response...")
    generated_ids = model.generate(
        **model_inputs,
        max_new_tokens=128,
        temperature=0.1
    )
    generated_ids = [
        output_ids[len(input_ids):] for input_ids, output_ids in zip(model_inputs.input_ids, generated_ids)
    ]
    response = tokenizer.batch_decode(generated_ids, skip_special_tokens=True)[0]
    
    print(f"Input: {sample['input']}")
    print(f"Output: {response}")
    
    # Log metric to MLflow
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("reminder-bot-experiment")
    
    # Try to load existing run_id
    run_id = None
    try:
        with open("last_run_id.txt", "r") as f:
            run_id = f.read().strip()
            print(f"Resuming MLflow Run: {run_id}")
    except FileNotFoundError:
        print("No last_run_id.txt found, starting new run.")

    with mlflow.start_run(run_id=run_id):
        mlflow.log_text(response, "test_sample_output.txt")
        # Simple validation: is it valid JSON?
        try:
            json.loads(response)
            mlflow.log_metric("test_json_valid", 1.0)
            print("Test Passed: Valid JSON")
        except:
            mlflow.log_metric("test_json_valid", 0.0)
            print("Test Failed: Invalid JSON")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_id", type=str, default="Qwen/Qwen2.5-3B-Instruct")
    parser.add_argument("--adapter_path", type=str, required=True)
    parser.add_argument("--test_data", type=str, required=True)
    args = parser.parse_args()
    
    test_model(args.model_id, args.adapter_path, args.test_data)
