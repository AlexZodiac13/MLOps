from quality_gate import evaluate_quality_gate


def test_quality_gate_passes_on_valid_metrics():
    metrics = {
        "quality_weighted_f1": 0.9,
        "quality_p95_latency_sec": 1.2,
        "quality_peak_ram_mb": 2048.0,
        "quality_json_valid_rate": 0.99,
        "quality_exact_match_rate": 0.80,
    }
    result = evaluate_quality_gate(
        metrics=metrics,
        min_weighted_f1=0.85,
        max_latency_p95_sec=1.5,
        max_ram_mb=4096.0,
        min_json_valid_rate=0.95,
        min_exact_match_rate=0.60,
    )
    assert result.passed is True
    assert result.errors == []


def test_quality_gate_fails_on_thresholds():
    metrics = {
        "quality_weighted_f1": 0.8,
        "quality_p95_latency_sec": 2.0,
        "quality_peak_ram_mb": 5000.0,
        "quality_json_valid_rate": 0.90,
        "quality_exact_match_rate": 0.50,
    }
    result = evaluate_quality_gate(
        metrics=metrics,
        min_weighted_f1=0.85,
        max_latency_p95_sec=1.5,
        max_ram_mb=4096.0,
        min_json_valid_rate=0.95,
        min_exact_match_rate=0.60,
    )
    assert result.passed is False
    assert len(result.errors) == 5
