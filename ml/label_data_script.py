#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Labeling Script with Teacher Model.
Использует большую модель (Qwen2.5-7B-Instruct в 4-bit) для лейблинга сырых данных.
"""

import argparse
import json
import os
import re
import sys
import time
import threading

import pandas as pd
import torch
from tqdm.auto import tqdm
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
)


def generate_label(message: str, created_at: str, model, tokenizer) -> str:
    system_prompt = """Ты — система для извлечения параметров напоминаний из пользовательского текста.
Твоя задача:
1. Определить текст напоминания (что нужно сделать). 
2. Определить дату (date) в формате YYYY-MM-DD.
3. Определить время (time) в формате HH:MM.
4. Определить периодичность (repeat): 'daily', 'weekly', 'monthly', 'yearly' или 'none'.
5. Преобразовать относительные даты ("завтра", "через 2 часа") в абсолютные, используя текущую дату (Context Date).
6. Вернуть результат СТРОГО в формате JSON. Не добавляй никаких объяснений.

Формат JSON:
{
  "text": "...",
  "date": "YYYY-MM-DD",
  "time": "HH:MM",
  "repeat": "none"
}
Если чего-то нет, ставь null.
"""
    
    user_prompt = f'Context Date: {created_at}\nMessage: "{message}"\n\nJSON:'
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    
    text = tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True
    )
    
    model_inputs = tokenizer([text], return_tensors="pt").to(model.device)
    
    generated_ids = model.generate(
        **model_inputs,
        max_new_tokens=256,
        do_sample=False
    )
    
    generated_ids = [
        output_ids[len(input_ids):] 
        for input_ids, output_ids in zip(model_inputs.input_ids, generated_ids)
    ]
    
    response = tokenizer.batch_decode(generated_ids, skip_special_tokens=True)[0]
    return response


def extract_json(response: str) -> dict:
    try:
        match = re.search(r'\{.*\}', response, re.DOTALL)
        if match:
            return json.loads(match.group(0))
        return json.loads(response)
    except:
        return None


def label_data(
    input_path: str,
    output_path: str,
    model_id: str,
    limit: int = None,
) -> None:
    print(f"CUDA Available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        print(f"GPU: {torch.cuda.get_device_name(0)}")
        print(f"VRAM: {torch.cuda.get_device_properties(0).total_memory / 1024**3:.2f} GB")

    # Загрузка данных
    print(f"Loading data from {input_path}...")
    df = pd.read_csv(input_path)
    
    # Фильтрация пустых/коротких сообщений
    df = df.dropna(subset=['message_text'])
    df = df[df['message_text'].str.len() > 5]
    print(f"Rows after filtering: {len(df)}")

    # Лимит для тестового режима
    if limit is not None and limit > 0:
        print(f"TEST MODE: Limiting to {limit} samples")
        df = df.head(limit)

    # Загрузка модели
    print(f"Loading model: {model_id}")
    bnb_config = BitsAndBytesConfig(
        load_in_4bit=True,
        bnb_4bit_quant_type="nf4",
        bnb_4bit_compute_dtype=torch.float16,
    )
    
    tokenizer = AutoTokenizer.from_pretrained(model_id)
    model = AutoModelForCausalLM.from_pretrained(
        model_id,
        quantization_config=bnb_config,
        device_map="auto",
        trust_remote_code=True
    )
    print("Model loaded.")

    # Лейблинг
    labeled_data = []
    success_count = 0
    
    # Heartbeat для долгих задач
    upload_started = time.perf_counter()
    stop_event = threading.Event()
    
    def _heartbeat():
        while not stop_event.wait(60):
            elapsed = time.perf_counter() - upload_started
            print(f"Labeling in progress... elapsed={elapsed:.1f}s, processed={success_count}/{len(df)}", flush=True)
    
    heartbeat = threading.Thread(target=_heartbeat, daemon=True)
    heartbeat.start()
    
    print(f"Labeling {len(df)} samples...")
    for idx, row in tqdm(df.iterrows(), total=len(df)):
        msg = row['message_text']
        created_at = row.get('created_at', '2026-02-18')
        
        try:
            response = generate_label(msg, str(created_at), model, tokenizer)
            json_data = extract_json(response)
            
            if json_data:
                entry = {
                    "input": msg,
                    "context_date": str(created_at),
                    "output": json_data
                }
                labeled_data.append(entry)
                success_count += 1
        except Exception as e:
            continue
    
    stop_event.set()
    heartbeat.join(timeout=1)
    
    print(f"Finished. Successfully labeled: {success_count}/{len(df)}")

    # Сохранение
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        print(f"Output directory: {output_dir}")
        print(f"Current working directory: {os.getcwd()}")
    
    print(f"Saving to: {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(labeled_data, f, ensure_ascii=False, indent=2)

    # Проверка
    if os.path.exists(output_path):
        size = os.path.getsize(output_path)
        print(f"Saved successfully: {output_path} ({size} bytes)")
    else:
        print(f"ERROR: File was not created: {output_path}")
        raise FileNotFoundError(f"Failed to create {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Label user messages with teacher model")
    parser.add_argument("--input", type=str, required=True, help="Path to input CSV")
    parser.add_argument("--output", type=str, required=True, help="Path to output JSON")
    parser.add_argument("--model_id", type=str, default="Qwen/Qwen2.5-7B-Instruct")
    parser.add_argument("--limit", type=int, default=None, help="Limit samples for testing (e.g., 100)")
    args = parser.parse_args()

    label_data(args.input, args.output, args.model_id, limit=args.limit)
