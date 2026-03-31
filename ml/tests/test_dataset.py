import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATASET = ROOT / "labeled_dataset.json"


def test_dataset_exists_and_not_empty():
    assert DATASET.exists()
    data = json.loads(DATASET.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert len(data) > 0


def test_dataset_schema_and_basic_formats():
    data = json.loads(DATASET.read_text(encoding="utf-8"))
    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    time_pattern = re.compile(r"^\d{2}:\d{2}$")

    for sample in data:
        assert "input" in sample
        assert "context_date" in sample
        assert "output" in sample

        output = sample["output"]
        assert "text" in output
        assert "date" in output
        assert "time" in output
        assert "repeat" in output

        if output["date"] is not None:
            assert date_pattern.match(output["date"])
        if output["time"] is not None:
            assert time_pattern.match(output["time"])
