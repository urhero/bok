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


def test_verdict_uncategorized_pattern():
    """UNCATEGORIZED 패턴은 동명 verdict 로 전달 (spec §5.2)."""
    assert classify_verdict("UNCATEGORIZED", 0.50) == "UNCATEGORIZED"


def test_verdict_boundary_at_0_60_is_warn():
    """경계값 pctile == 0.60 는 PERCENTILE_WARN (>= 세만틱 고정)."""
    assert classify_verdict("NORMAL", 0.60) == "PERCENTILE_WARN"


from scripts.run_cluster_turnover_experiment import build_summary_row


def _fake_overfit_report() -> dict:
    return {
        "funnel_pattern": "NORMAL",
        "funnel_ew_all_cagr": 0.05,
        "funnel_ew_top50_cagr": 0.08,
        "funnel_cew_cagr": 0.10,
        "oos_avg_percentile": 0.42,
        "strict_jaccard": 0.55,
        "is_oos_rank_spearman": 0.35,
        "deflation_ratio": 0.70,
        "oos_cagr": 0.10,
        "oos_mdd": -0.25,
        "oos_sharpe": 1.1,
        "oos_calmar": 0.4,
        "oos_ew_cagr": 0.08,
        "oos_ew_sharpe": 0.9,
        "funnel_interpretation": "...",
        "oos_percentile_interpretation": "...",
        "strict_jaccard_interpretation": "...",
        "rank_corr_interpretation": "...",
        "deflation_interpretation": "...",
    }


def test_build_summary_row_ok_case():
    case = {"name": "cluster_18", "override": {
        "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
    }, "alpha": 1.0}
    row = build_summary_row(
        case=case,
        overfit_report=_fake_overfit_report(),
        avg_turnover=0.18,
        runtime_sec=123.4,
        status="OK",
        error=None,
    )
    assert row["case"] == "cluster_18"
    assert row["use_cluster_dedup"] is True
    assert row["n_clusters"] == 18
    assert row["per_cluster_keep"] == 3
    assert row["turnover_alpha"] == 1.0
    assert row["status"] == "OK"
    assert row["cagr_cew"] == 0.10
    assert row["sharpe_cew"] == 1.1
    assert row["avg_turnover"] == 0.18
    assert row["funnel_verdict"].startswith("OK")
    assert row["oos_pctile_flag"] == "OK"
    assert row["verdict"] == "OK"
    assert row["runtime_sec"] == 123.4


def test_build_summary_row_baseline_has_default_cluster_fields():
    case = {"name": "baseline", "override": {}, "alpha": 1.0}
    row = build_summary_row(
        case=case,
        overfit_report=_fake_overfit_report(),
        avg_turnover=0.25,
        runtime_sec=100.0,
        status="OK",
        error=None,
    )
    assert row["use_cluster_dedup"] is False
    # baseline 은 override 에 cluster 필드가 없으므로 기본값 또는 NaN
    assert row["n_clusters"] is None or np.isnan(row["n_clusters"])


def test_build_summary_row_failed_case():
    case = {"name": "cluster_18", "override": {
        "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
    }, "alpha": 1.0}
    row = build_summary_row(
        case=case,
        overfit_report=None,
        avg_turnover=float("nan"),
        runtime_sec=5.0,
        status="FAILED",
        error="ZeroDivisionError: ...",
    )
    assert row["status"] == "FAILED"
    assert row["error"] == "ZeroDivisionError: ..."
    assert np.isnan(row["cagr_cew"])
    assert row["verdict"] == "N/A"


def test_build_summary_row_percentile_warn():
    report = _fake_overfit_report()
    report["oos_avg_percentile"] = 0.70  # >= 0.60
    case = {"name": "baseline", "override": {}, "alpha": 1.0}
    row = build_summary_row(case, report, 0.3, 90.0, "OK", None)
    assert row["oos_pctile_flag"] == "WARN"
    assert row["verdict"] == "PERCENTILE_WARN"


def test_build_summary_row_computes_net_cagr_cew():
    """net_cagr_cew = cagr_cew - avg_turnover * tc_annual_rate."""
    case = {"name": "cluster_18", "override": {
        "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
    }, "alpha": 0.7}
    report = _fake_overfit_report()
    # oos_cagr = 0.10 (from fixture), avg_turnover = 0.25, tc_annual_rate = 0.012 (default)
    # expected net = 0.10 - 0.25 * 0.012 = 0.10 - 0.003 = 0.097
    row = build_summary_row(case, report, 0.25, 100.0, "OK", None)
    assert abs(row["net_cagr_cew"] - 0.097) < 1e-9


def test_build_summary_row_net_cagr_respects_custom_tc_rate():
    """tc_annual_rate 를 override 하면 네트 CAGR 계산에 반영된다."""
    case = {"name": "baseline", "override": {}, "alpha": 1.0}
    report = _fake_overfit_report()  # oos_cagr = 0.10
    # avg_turnover = 0.5, tc_annual_rate = 0.02 -> net = 0.10 - 0.5 * 0.02 = 0.09
    row = build_summary_row(case, report, 0.5, 100.0, "OK", None, tc_annual_rate=0.02)
    assert abs(row["net_cagr_cew"] - 0.09) < 1e-9


def test_build_summary_row_failed_has_nan_net_cagr():
    """FAILED 케이스는 net_cagr_cew 도 NaN."""
    case = {"name": "x", "override": {}, "alpha": 1.0}
    row = build_summary_row(case, None, float("nan"), 1.0, "FAILED", "err")
    assert np.isnan(row["net_cagr_cew"])
