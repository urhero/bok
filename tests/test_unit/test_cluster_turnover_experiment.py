# -*- coding: utf-8 -*-
"""scripts/run_cluster_turnover_experiment.py 헬퍼 함수 단위 테스트."""
from __future__ import annotations

import sys
from pathlib import Path

# scripts/ 는 패키지가 아니므로 sys.path 에 추가
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.run_cluster_turnover_experiment import build_cases


def test_build_cases_returns_8_cases():
    cases = build_cases()
    assert len(cases) == 8


def test_case_names_are_unique():
    cases = build_cases()
    names = [c["name"] for c in cases]
    assert len(set(names)) == 8


def test_baseline_case_has_empty_override_and_alpha_1():
    cases = build_cases()
    baseline = next(c for c in cases if c["name"] == "baseline")
    assert baseline["override"] == {}
    assert baseline["alpha"] == 1.0


def test_cluster_18_case_has_correct_override():
    cases = build_cases()
    case = next(c for c in cases if c["name"] == "cluster_18")
    assert case["override"] == {
        "use_cluster_dedup": True,
        "n_clusters": 18,
        "per_cluster_keep": 3,
    }
    assert case["alpha"] == 1.0


def test_combo_strong_case():
    cases = build_cases()
    case = next(c for c in cases if c["name"] == "combo_18_0.5")
    assert case["override"]["use_cluster_dedup"] is True
    assert case["override"]["n_clusters"] == 18
    assert case["alpha"] == 0.5


import pandas as pd
import numpy as np

from scripts.run_cluster_turnover_experiment import compute_avg_turnover


def test_compute_avg_turnover_empty_history():
    """빈 weight_history 는 NaN 반환."""
    wh = pd.DataFrame()
    assert np.isnan(compute_avg_turnover(wh))


def test_compute_avg_turnover_single_rebalance():
    """단일 리밸런싱 시점은 diff 불가 → NaN."""
    wh = pd.DataFrame(
        {"factorA": [0.5], "factorB": [0.5]},
        index=pd.to_datetime(["2020-01-31"]),
    )
    assert np.isnan(compute_avg_turnover(wh))


def test_compute_avg_turnover_identical_weights_zero():
    """가중치 변화 없는 연속 리밸런싱 → 0."""
    wh = pd.DataFrame(
        {"factorA": [0.5, 0.5, 0.5], "factorB": [0.5, 0.5, 0.5]},
        index=pd.to_datetime(["2020-01-31", "2020-04-30", "2020-07-31"]),
    )
    assert compute_avg_turnover(wh) == 0.0


def test_compute_avg_turnover_full_swap():
    """A 100% -> B 100% 로 전환 시 turnover = 1.0 (L1/2)."""
    wh = pd.DataFrame(
        {"factorA": [1.0, 0.0], "factorB": [0.0, 1.0]},
        index=pd.to_datetime(["2020-01-31", "2020-04-30"]),
    )
    # |1-0| + |0-1| = 2, /2 = 1.0
    assert compute_avg_turnover(wh) == 1.0


def test_compute_avg_turnover_with_nan_factors():
    """새로 등장한 팩터는 이전 가중치 0 으로 간주."""
    wh = pd.DataFrame({
        "factorA": [0.5, np.nan],   # A 사라짐
        "factorB": [0.5, 0.5],
        "factorC": [np.nan, 0.5],   # C 새로 등장
    }, index=pd.to_datetime(["2020-01-31", "2020-04-30"]))
    # diff: |0-0.5| + |0.5-0.5| + |0.5-0| = 1.0, /2 = 0.5
    result = compute_avg_turnover(wh)
    assert abs(result - 0.5) < 1e-9


from scripts.run_cluster_turnover_experiment import classify_verdict


def test_verdict_ok_when_normal_and_low_pctile():
    assert classify_verdict("NORMAL", 0.45) == "OK"


def test_verdict_percentile_warn_when_normal_but_high_pctile():
    assert classify_verdict("NORMAL", 0.65) == "PERCENTILE_WARN"


def test_verdict_optimization_overfit():
    # pattern 이 OPTIMIZATION_OVERFIT 이면 pctile 무시
    assert classify_verdict("OPTIMIZATION_OVERFIT", 0.30) == "OPTIMIZATION_OVERFIT"


def test_verdict_filter_overfit():
    assert classify_verdict("FILTER_OVERFIT", 0.30) == "FILTER_OVERFIT"


def test_verdict_insufficient_data_returns_na():
    assert classify_verdict("INSUFFICIENT_DATA", float("nan")) == "N/A"


def test_verdict_nan_pctile_with_normal_returns_ok():
    # pctile 계산 불가 시 (NaN) → 패턴만으로 판단
    assert classify_verdict("NORMAL", float("nan")) == "OK"
