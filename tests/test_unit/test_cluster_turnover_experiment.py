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


import tempfile

from scripts.run_cluster_turnover_experiment import render_markdown_report


def _fake_summary_df():
    # 2 케이스만 있는 축소판 (렌더링만 검증)
    rows = [
        {
            "case": "baseline",
            "use_cluster_dedup": False, "n_clusters": None, "per_cluster_keep": None,
            "turnover_alpha": 1.0,
            "status": "OK", "error": None, "runtime_sec": 100.0,
            "cagr_cew": 0.08, "sharpe_cew": 0.9, "mdd_cew": -0.30,
            "calmar_cew": 0.27, "cagr_ew": 0.07, "sharpe_ew": 0.8,
            "net_cagr_cew": 0.076,  # 0.08 - 0.35 * 0.012 ~= 0.076
            "avg_turnover": 0.35,
            "funnel_a_cagr": 0.04, "funnel_b_cagr": 0.06, "funnel_c_cagr": 0.08,
            "oos_pctile_value": 0.50, "oos_pctile_flag": "OK",
            "strict_jaccard": 0.40, "is_oos_rank_corr": 0.30, "deflation_ratio": 0.65,
            "funnel_verdict": "OK (C>B>A)", "verdict": "OK",
        },
        {
            "case": "cluster_18",
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
            "turnover_alpha": 1.0,
            "status": "OK", "error": None, "runtime_sec": 110.0,
            "cagr_cew": 0.10, "sharpe_cew": 1.1, "mdd_cew": -0.25,
            "calmar_cew": 0.40, "cagr_ew": 0.07, "sharpe_ew": 0.8,
            "net_cagr_cew": 0.0964,  # 0.10 - 0.30 * 0.012 = 0.0964
            "avg_turnover": 0.30,
            "funnel_a_cagr": 0.04, "funnel_b_cagr": 0.08, "funnel_c_cagr": 0.10,
            "oos_pctile_value": 0.45, "oos_pctile_flag": "OK",
            "strict_jaccard": 0.50, "is_oos_rank_corr": 0.35, "deflation_ratio": 0.70,
            "funnel_verdict": "OK (C>B>A)", "verdict": "OK",
        },
    ]
    return pd.DataFrame(rows)


def test_render_markdown_report_creates_file():
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "REPORT.md"
        render_markdown_report(
            _fake_summary_df(),
            out,
            meta={"git_sha": "abc123", "start": "2020-01-01", "end": "2020-12-31", "workers": 2},
        )
        assert out.exists()


def test_render_markdown_report_contains_key_sections():
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "REPORT.md"
        render_markdown_report(
            _fake_summary_df(),
            out,
            meta={"git_sha": "abc123", "start": "2020-01-01", "end": "2020-12-31", "workers": 2},
        )
        text = out.read_text(encoding="utf-8")
        assert "# Cluster Dedup" in text or "# Hierarchical" in text
        assert "## 1. 성과 요약" in text
        assert "## 2. 과적합 진단" in text
        assert "## 3. 해석" in text
        assert "## 4. 추천 조합" in text
        assert "## 5. 실행 메타" in text
        # 케이스 이름이 표에 나와야 함
        assert "baseline" in text
        assert "cluster_18" in text


def test_render_markdown_report_recommendation_picks_highest_sharpe_ok():
    # 두 케이스 모두 verdict=OK, cluster_18 의 sharpe 가 더 높음 -> 추천 = cluster_18
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "REPORT.md"
        render_markdown_report(
            _fake_summary_df(),
            out,
            meta={"git_sha": "abc123", "start": "2020-01-01", "end": "2020-12-31", "workers": 2},
        )
        text = out.read_text(encoding="utf-8")
        # "최종 추천" 섹션이 cluster_18 을 포함해야
        rec_section = text.split("## 4. 추천 조합")[1].split("## 5.")[0]
        assert "cluster_18" in rec_section


from scripts.run_cluster_turnover_experiment import pick_recommendation


def test_pick_recommendation_returns_none_when_no_ok_rows():
    """모든 케이스가 FAILED 또는 과적합 verdict 일 때 None 반환."""
    df = pd.DataFrame([
        {"case": "a", "status": "FAILED", "verdict": "N/A", "sharpe_cew": np.nan, "avg_turnover": np.nan},
        {"case": "b", "status": "OK", "verdict": "OPTIMIZATION_OVERFIT", "sharpe_cew": 1.0, "avg_turnover": 0.1},
    ])
    assert pick_recommendation(df) is None


def test_pick_recommendation_picks_highest_sharpe_min_turnover():
    """verdict=OK 중 Sharpe 상위 3개 -> 그 중 avg_turnover 최저."""
    df = pd.DataFrame([
        {"case": "low_sharpe", "status": "OK", "verdict": "OK", "sharpe_cew": 0.5, "avg_turnover": 0.01},
        {"case": "top_sharpe_high_to", "status": "OK", "verdict": "OK", "sharpe_cew": 1.2, "avg_turnover": 0.50},
        {"case": "top_sharpe_low_to", "status": "OK", "verdict": "OK", "sharpe_cew": 1.1, "avg_turnover": 0.05},
        {"case": "mid_sharpe", "status": "OK", "verdict": "OK", "sharpe_cew": 1.0, "avg_turnover": 0.15},
    ])
    # top3 by Sharpe: top_sharpe_high_to (1.2), top_sharpe_low_to (1.1), mid_sharpe (1.0)
    # min turnover among top3: top_sharpe_low_to (0.05)
    best = pick_recommendation(df)
    assert best is not None
    assert best["case"] == "top_sharpe_low_to"


def test_pick_recommendation_single_ok_row_returned():
    """OK 행이 1개뿐이어도 그것을 반환."""
    df = pd.DataFrame([
        {"case": "only", "status": "OK", "verdict": "OK", "sharpe_cew": 0.7, "avg_turnover": 0.2},
        {"case": "bad", "status": "OK", "verdict": "FILTER_OVERFIT", "sharpe_cew": 2.0, "avg_turnover": 0.01},
    ])
    best = pick_recommendation(df)
    assert best is not None
    assert best["case"] == "only"
