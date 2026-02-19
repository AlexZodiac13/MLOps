#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Tuple


SUSPICIOUS_PATTERNS = [
    r"\bвзлом\b",
    r"\bвзломать\b",
    r"\bcrack\b",
    r"\bhack\b",
    r"\bpirate\b",
    r"\bkeygen\b",
    r"\bwarez\b",
]


def is_suspicious(text: str) -> bool:
    lowered = text.lower()
    return any(re.search(p, lowered, flags=re.IGNORECASE) for p in SUSPICIOUS_PATTERNS)


RUS_COMMON_TYPO_MAP = {
    "сегодня": ["сеголня", "сигодня", "седня", "севодня"],
    "завтра": ["завтро", "завтраа", "завра"],
    "проверить": ["провертиь", "провертить", "проверитьь"],
    "оплата": ["аплата", "оплта", "оплатта"],
    "каждый": ["кажды", "каждыйй", "каждый ", "каждыи"],
    "каждую": ["каждую ", "каждуюю", "каджую"],
    "каждое": ["каждое ", "каждоее", "каджое"],
    "четверг": ["четвер", "четврег", "четвег"],
    "среду": ["сред", "средуу"],
    "воскресенье": ["воскресение", "воскр", "воскресенье "],
}


def random_case_noise(s: str, rng: random.Random) -> str:
    if not s:
        return s
    mode = rng.choice(["none", "lower", "upper", "title", "swap"])
    if mode == "none":
        return s
    if mode == "lower":
        return s.lower()
    if mode == "upper":
        return s.upper()
    if mode == "title":
        return s[:1].upper() + s[1:]
    # swap
    out = []
    for ch in s:
        if ch.isalpha() and rng.random() < 0.07:
            out.append(ch.upper() if ch.islower() else ch.lower())
        else:
            out.append(ch)
    return "".join(out)


def random_whitespace_noise(s: str, rng: random.Random) -> str:
    # Add/remove some spaces, but keep overall readability
    s = re.sub(r"[ \t]+", " ", s)
    if rng.random() < 0.2:
        s = s.replace(" ", "  ", 1)
    if rng.random() < 0.2:
        s = s.replace(" ", " ", 1)
    if rng.random() < 0.15:
        s = s.strip() + (" " if rng.random() < 0.5 else "")
    return s


TIME_PATTERNS: List[Tuple[re.Pattern, List[str]]] = [
    # 10-00 / 10-0 / 10-000 variants
    (re.compile(r"\b(\d{1,2})-(\d{2})\b"), ["{h}:{m}", "{h}.{m}", "{h} {m}", "{h}-{m}", "{h}:{m} " ]),
    # 10:00 variants
    (re.compile(r"\b(\d{1,2}):(\d{2})\b"), ["{h}-{m}", "{h}.{m}", "{h} {m}", "{h}:{m}", "{h}:{m} " ]),
    # 0:05 variants
    (re.compile(r"\b(\d{1,2}):(\d{2})\b"), ["{h}:{m}", "{h}ч{m}", "{h}-{m}"]),
]


def normalize_hour(h: str) -> str:
    try:
        return str(int(h))
    except Exception:
        return h


def time_format_noise(s: str, rng: random.Random) -> str:
    def _repl(match: re.Match) -> str:
        h, m = match.group(1), match.group(2)
        template = rng.choice(["{h}:{m}", "{h}-{m}", "{h}.{m}", "{h} {m}", "{h}:{m}"])
        # occasionally drop leading zero in minutes (intentional error)
        mm = m
        if rng.random() < 0.08 and mm.startswith("0"):
            mm = mm[1:]
        # occasionally make minutes wrong length (intentional error)
        if rng.random() < 0.03:
            mm = mm + rng.choice(["0", "00"])
        return template.format(h=normalize_hour(h), m=mm)

    # Apply 0-2 random time substitutions
    out = s
    for _ in range(rng.randint(0, 2)):
        pattern = rng.choice(TIME_PATTERNS)[0]
        if pattern.search(out):
            out = pattern.sub(_repl, out, count=1)
    return out


def russian_common_typos(s: str, rng: random.Random) -> str:
    out = s
    lowered = out.lower()

    # Replace a common word with a typo variant
    candidates = [k for k in RUS_COMMON_TYPO_MAP.keys() if k in lowered]
    if candidates and rng.random() < 0.35:
        key = rng.choice(candidates)
        replacement = rng.choice(RUS_COMMON_TYPO_MAP[key])
        # do a case-preserving-ish replacement once
        out = re.sub(re.escape(key), replacement, out, flags=re.IGNORECASE, count=1)

    # Occasionally swap two adjacent characters in a random word
    if rng.random() < 0.15:
        words = re.findall(r"[A-Za-zА-Яа-яЁё]{4,}", out)
        if words:
            w = rng.choice(words)
            if len(w) >= 5:
                i = rng.randint(1, len(w) - 2)
                w2 = w[:i] + w[i + 1] + w[i] + w[i + 2 :]
                out = out.replace(w, w2, 1)

    return out


def punctuation_noise(s: str, rng: random.Random) -> str:
    out = s
    if rng.random() < 0.2:
        out = out.replace(":", " :", 1)
    if rng.random() < 0.2:
        out = out.replace("-", "–", 1)
    if rng.random() < 0.15:
        out = out.replace("!", "", 1)
    if rng.random() < 0.1:
        out = out + rng.choice([".", "..", "!", ""])
    return out


def link_noise(s: str, rng: random.Random) -> str:
    # Mildly break some links (remove scheme, add space) to simulate user typos.
    out = s
    if "http" in out and rng.random() < 0.18:
        out = out.replace("https://", "http://", 1) if rng.random() < 0.5 else out.replace("https://", "", 1)
    if "t.me" in out and rng.random() < 0.12:
        out = out.replace("t.me/", "t.me /", 1)
    if "wiki." in out and rng.random() < 0.10:
        out = out.replace("wiki.", "wki.", 1)
    return out


def augment_text(text: str, rng: random.Random) -> str:
    out = text

    # For multiline blocks keep structure; just small noise
    out = time_format_noise(out, rng)
    out = russian_common_typos(out, rng)
    out = random_whitespace_noise(out, rng)
    out = punctuation_noise(out, rng)
    out = link_noise(out, rng)
    out = random_case_noise(out, rng)

    return out


def parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def jitter_dt(base: datetime, rng: random.Random) -> datetime:
    # Randomly shift time within +/- 72 hours and up to 30 minutes
    shift_days = rng.randint(-3, 3)
    shift_minutes = rng.randint(-30, 30)
    shift_seconds = rng.randint(-30, 30)
    return base + timedelta(days=shift_days, minutes=shift_minutes, seconds=shift_seconds)


@dataclass
class Row:
    user_id: str
    chat_id: str
    message_text: str
    message_type: str
    created_at: str


def load_rows(path: str) -> List[Row]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows: List[Row] = []
        for r in reader:
            rows.append(
                Row(
                    user_id=str(r["user_id"]),
                    chat_id=str(r["chat_id"]),
                    message_text=str(r["message_text"]),
                    message_type=str(r.get("message_type") or "text"),
                    created_at=str(r["created_at"]),
                )
            )
        return rows


def write_rows(path: str, rows: Iterable[Dict[str, str]]) -> None:
    fieldnames = ["id", "user_id", "chat_id", "message_text", "message_type", "created_at"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def generate(input_path: str, output_path: str, multiply: int, seed: int) -> None:
    rng = random.Random(seed)
    base_rows = load_rows(input_path)
    original_count = len(base_rows)
    target_count = original_count * multiply

    safe_rows = [r for r in base_rows if not is_suspicious(r.message_text)]
    suspicious_rows = [r for r in base_rows if is_suspicious(r.message_text)]

    out: List[Dict[str, str]] = []

    def add_row(new_id: int, source: Row, make_variant: bool) -> None:
        text = source.message_text
        created_at = source.created_at
        if make_variant:
            text = augment_text(text, rng)
            try:
                dt = parse_dt(created_at)
                created_at = jitter_dt(dt, rng).isoformat()
            except Exception:
                # keep as-is
                pass

        out.append(
            {
                "id": str(new_id),
                "user_id": source.user_id,
                "chat_id": source.chat_id,
                "message_text": text,
                "message_type": source.message_type or "text",
                "created_at": created_at,
            }
        )

    # Always include original rows once (including suspicious)
    next_id = 1
    for r in base_rows:
        add_row(next_id, r, make_variant=False)
        next_id += 1

    # Fill up to target_count by sampling only safe rows with replacement and generating variants.
    # This keeps dataset size exact without proliferating suspicious/illicit content.
    if not safe_rows:
        raise RuntimeError("No safe rows available for augmentation")

    while len(out) < target_count:
        src = rng.choice(safe_rows)
        add_row(next_id, src, make_variant=True)
        next_id += 1

    # Shuffle lightly but keep reproducible order
    rng.shuffle(out)
    # Reassign ids sequentially after shuffle
    for i, r in enumerate(out, start=1):
        r["id"] = str(i)

    write_rows(output_path, out)


def main() -> None:
    ap = argparse.ArgumentParser(description="Augment user_messages.csv by generating typo/noise variants")
    ap.add_argument("--input", default="user_messages.csv")
    ap.add_argument("--output", default="user_messages_augmented_x30.csv")
    ap.add_argument("--multiply", type=int, default=30)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    generate(args.input, args.output, args.multiply, args.seed)


if __name__ == "__main__":
    main()
