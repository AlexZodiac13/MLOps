import os
from fastapi import FastAPI, Request
from pydantic import BaseModel
from llama_cpp import Llama
import pandas as pd
import logging

MODEL_PATH = os.path.join(os.path.dirname(__file__), '../model/gguf/model_q4_k_m.gguf')

app = FastAPI()

# Логирование
logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), 'requests.log'),
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

from typing import Optional
from pydantic import Field

class ReminderRequest(BaseModel):
    message: Optional[str] = Field(None, alias="input")
    input: Optional[str] = None
    context_date: str = "2026-02-18"

    def get_message(self):
        return self.message or self.input

# Загрузка модели при старте
try:
    llm = Llama(
        model_path=MODEL_PATH,
        n_ctx=2048,
        n_threads=4,
        n_gpu_layers=0
    )
    print("Model loaded successfully on CPU.")
except Exception as e:
    print(f"Error loading model: {e}")
    llm = None

# Функция для извлечения параметров напоминания
def extract_reminder(text, context_date="2026-02-18"):
    if not llm:
        return {"error": "Model not loaded."}
    system_prompt = """Ты — система для извлечения параметров напоминаний.\nТвоя задача: извлечь текст, дату, время и периодичность из сообщения пользователя и вернуть JSON.\nИспользуй текущую дату (Context Date) для разрешения относительных дат.\nФормат: {\"text\": \"...\", \"date\": \"YYYY-MM-DD\", \"time\": \"HH:MM\", \"repeat\": \"...\"}"""
    user_prompt = f"Context Date: {context_date}\nMessage: \"{text}\"\n\nJSON:"
    prompt = f"<|im_start|>system\n{system_prompt}<|im_end|>\n<|im_start|>user\n{user_prompt}<|im_end|>\n<|im_start|>assistant\n"
    output = llm(
        prompt,
        max_tokens=256,
        stop=["<|im_end|>"],
        temperature=0.1,
        echo=False
    )
    return output['choices'][0]['text']

from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Разрешить CORS для тестирования
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"] ,
    allow_headers=["*"]
)

@app.post("/extract_reminder")
async def extract_reminder_api(payload: ReminderRequest):
    logging.info(f"Request: {payload.dict()}")
    msg = payload.get_message()
    result_raw = extract_reminder(msg, payload.context_date)
    logging.info(f"Response: {result_raw}")
    # Попытаться распарсить JSON
    import json
    try:
        result = json.loads(result_raw)
    except Exception:
        result = {"text": msg, "date": None, "time": None, "repeat": None}
    return JSONResponse(content={"output": result})

@app.get("/")
def root():
    return {"status": "ok", "message": "Reminder extraction API is running."}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=True)
