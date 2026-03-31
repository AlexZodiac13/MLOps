from evaluation import evaluate_predictions, parse_prediction


def test_parse_prediction_extracts_json_object():
    parsed, error = parse_prediction('prefix {"text":"a","date":"2026-01-01","time":"10:00","repeat":null} suffix')
    assert error is None
    assert parsed["text"] == "a"
    assert parsed["date"] == "2026-01-01"


def test_parse_prediction_extracts_json_from_markdown_code_block():
    raw = (
        "Ответ:\n"
        "```json\n"
        "{\"text\":\"buy milk\",\"date\":\"2026-01-01\",\"time\":\"09:00\",\"repeat\":null}\n"
        "```\n"
    )
    parsed, error = parse_prediction(raw)
    assert error is None
    assert parsed["text"] == "buy milk"
    assert parsed["time"] == "09:00"


def test_evaluate_predictions_reports_weighted_f1():
    gold = [
        {
            "output": {
                "text": "call mom",
                "date": "2026-01-01",
                "time": "10:00",
                "repeat": "daily",
            }
        },
        {
            "output": {
                "text": "meeting",
                "date": "2026-01-02",
                "time": "15:30",
                "repeat": None,
            }
        },
    ]
    predictions = [
        {
            "parsed": {
                "text": "call mom",
                "date": "2026-01-01",
                "time": "10:00",
                "repeat": "daily",
            }
        },
        {
            "parsed": {
                "text": "meeting",
                "date": "2026-01-02",
                "time": "15:30",
                "repeat": "weekly",
            }
        },
    ]

    metrics = evaluate_predictions(gold, predictions, latencies=[0.5, 1.0])

    assert metrics["date_f1"] == 1.0
    assert metrics["time_f1"] == 1.0
    assert metrics["repeat_f1"] < 1.0
    assert metrics["quality_weighted_f1"] >= 0.9
    assert metrics["latency_p95_sec"] >= metrics["latency_avg_sec"]


def test_evaluate_predictions_normalizes_date_time_and_repeat():
    gold = [
        {
            "output": {
                "text": "pay rent",
                "date": "2026-03-05",
                "time": "09:00",
                "repeat": "daily",
            }
        }
    ]
    predictions = [
        {
            "parsed": {
                "text": "pay rent",
                "date": "2026-3-5",
                "time": "9:00:00",
                "repeat": "ежедневно",
            }
        }
    ]

    metrics = evaluate_predictions(gold, predictions, latencies=[0.7])

    assert metrics["json_valid_rate"] == 1.0
    assert metrics["exact_match_rate"] == 1.0
