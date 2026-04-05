# -*- coding: utf-8 -*-
"""과적합 진단 모듈 단위 테스트.

3단계 핵심 테스트 + 보조 지표 검증.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from service.backtest.overfit_diagnostics import (
    calc_deflation_ratio,
    calc_funnel_value_add,
    calc_is_oos_rank_correlation,
    calc_oos_percentile_tracking,
    calc_strict_jaccard,
)
from service.backtest.result_stitcher import WalkForwardResult


def _make_result(
    n_months=12,
    mp_ret=0.01,
    ew_ret=0.005,
    with_meta=True,
    all_factor_returns=None,
    top50_factors=None,
    active_factors=None,
):
    """간단한 WalkForwardResult 생성 헬퍼.

    결정적 테스트를 위해 시드를 고정한다.

    Args:
        all_factor_returns: 전체 팩터 수익률 dict (EW_All/Percentile용).
            None이면 기본값 사용.
        top50_factors: Top-50 팩터 리스트 (EW_Top50용).
        active_factors: weight>0 팩터 리스트 (Strict Jaccard용).
    """
    np.random.seed(42)
    dates = pd.date_range("2021-01-31", periods=n_months, freq="ME")
    default_all_fr = {
        "F1": 0.012, "F2": 0.008, "F3": 0.005,
        "F4": 0.003, "F5": -0.002, "F6": -0.005,
        "F7": 0.001, "F8": -0.001, "F9": 0.004, "F10": 0.002,
    }
    default_top50 = ["F1", "F2", "F3", "F4", "F5"]
    default_active = ["F1", "F2", "F3"]

    results = []
    for i, d in enumerate(dates):
        entry = {
            "date": d,
            "oos_return": mp_ret + np.random.randn() * 0.001,
            "oos_ew_return": ew_ret + np.random.randn() * 0.001,
            "oos_factor_returns": {"F1": 0.01, "F2": 0.005, "F3": 0.008},
            "weights": {"F1": 0.5, "F2": 0.3, "F3": 0.2},
            "is_meta": None,
            "is_rule_rebal": (i % 6 == 0),
            "is_weight_rebal": (i % 3 == 0),
            "oos_all_factor_returns": all_factor_returns if all_factor_returns else default_all_fr,
            "top50_factors": top50_factors if top50_factors else default_top50,
            "active_factors": active_factors if active_factors else default_active,
        }
        if with_meta and i % 3 == 0:
            entry["is_meta"] = pd.DataFrame({
                "factorAbbreviation": ["F1", "F2", "F3"],
                "cagr": [0.15, 0.10, 0.05],
                "styleName": ["Val", "Mom", "Qual"],
            })
        results.append(entry)
    return WalkForwardResult(results)


# ── 1순위: Funnel Value-Add Test ──


class TestFunnelValueAdd:
    def test_normal_pattern(self):
        """C > B > A → NORMAL 패턴."""
        # MP가 가장 높고, Top50이 중간, All이 가장 낮은 수익률
        result = _make_result(
            n_months=12, mp_ret=0.015, ew_ret=0.01,
            all_factor_returns={
                "F1": 0.020, "F2": 0.015, "F3": 0.012,
                "F4": 0.001, "F5": -0.005, "F6": -0.008,
                "F7": -0.003, "F8": -0.004, "F9": -0.002, "F10": 0.000,
            },
            top50_factors=["F1", "F2", "F3", "F4", "F5"],
        )
        funnel = calc_funnel_value_add(result)
        assert funnel["pattern"] == "NORMAL"
        assert funnel["mp_cagr"] > funnel["ew_top50_cagr"]

    def test_mc_overfit_pattern(self):
        """B > C > A → MC_OVERFIT 패턴.

        조건: EW_All(A) < EW_Top50(B) > MP_Final(C)
        - Top50에 고수익 팩터 배치 → EW_Top50 높음
        - 전체에 저수익 팩터 포함 → EW_All 낮음
        - MP는 Top50 EW보다 낮게 설정
        """
        result = _make_result(
            n_months=12, mp_ret=0.003, ew_ret=0.01,
            all_factor_returns={
                # Top50: 평균 +1.02%
                "F1": 0.015, "F2": 0.012, "F3": 0.010,
                "F4": 0.008, "F5": 0.006,
                # 나머지: 전체 평균을 끌어내림
                "F6": -0.020, "F7": -0.025, "F8": -0.030,
                "F9": -0.018, "F10": -0.022,
            },
            top50_factors=["F1", "F2", "F3", "F4", "F5"],
        )
        funnel = calc_funnel_value_add(result)
        assert funnel["pattern"] == "MC_OVERFIT"

    def test_filter_overfit_pattern(self):
        """A > B → FILTER_OVERFIT 패턴."""
        # 전체 팩터 EW가 Top-50 EW보다 높은 수익률
        result = _make_result(
            n_months=12, mp_ret=0.001,
            all_factor_returns={
                "F1": -0.005, "F2": -0.003, "F3": -0.002,
                "F4": -0.001, "F5": -0.004,
                "F6": 0.020, "F7": 0.015, "F8": 0.018,
                "F9": 0.012, "F10": 0.010,
            },
            top50_factors=["F1", "F2", "F3", "F4", "F5"],
        )
        funnel = calc_funnel_value_add(result)
        assert funnel["pattern"] == "FILTER_OVERFIT"

    def test_has_all_metrics(self):
        result = _make_result()
        funnel = calc_funnel_value_add(result)
        assert "ew_all_cagr" in funnel
        assert "ew_top50_cagr" in funnel
        assert "mp_cagr" in funnel
        assert "interpretation" in funnel

    def test_insufficient_data(self):
        """OOS 데이터 0건 → INSUFFICIENT_DATA 패턴."""
        result = WalkForwardResult([])
        funnel = calc_funnel_value_add(result)
        assert funnel["pattern"] == "INSUFFICIENT_DATA"
        assert "부족" in funnel["interpretation"]


# ── EW_All / EW_Top50 계산 로직 검증 ──


class TestEWCalculationLogic:
    def test_ew_all_is_mean_of_all_factors(self):
        """EW_All 수익률은 전체 팩터의 단순 평균이어야 한다."""
        all_fr = {"F1": 0.10, "F2": 0.20, "F3": 0.30}
        result = _make_result(n_months=3, all_factor_returns=all_fr, top50_factors=["F1"])
        # 각 월의 EW_All = mean(0.10, 0.20, 0.30) = 0.20
        for val in result.oos_ew_all_returns:
            assert abs(val - 0.20) < 1e-10

    def test_ew_top50_is_mean_of_top50_subset(self):
        """EW_Top50 수익률은 Top-50 팩터만의 평균이어야 한다."""
        all_fr = {"F1": 0.10, "F2": 0.20, "F3": 0.30, "F4": -0.50}
        result = _make_result(
            n_months=3,
            all_factor_returns=all_fr,
            top50_factors=["F1", "F2"],
        )
        # 각 월의 EW_Top50 = mean(0.10, 0.20) = 0.15
        for val in result.oos_ew_top50_returns:
            assert abs(val - 0.15) < 1e-10

    def test_ew_top50_excludes_non_top50(self):
        """EW_Top50은 Top-50에 없는 팩터를 제외해야 한다."""
        all_fr = {"F1": 0.10, "F2": -0.50}
        result = _make_result(
            n_months=3,
            all_factor_returns=all_fr,
            top50_factors=["F1"],
        )
        # EW_Top50 = mean(0.10) = 0.10 (F2 제외)
        for val in result.oos_ew_top50_returns:
            assert abs(val - 0.10) < 1e-10


# ── 2순위: OOS Percentile Tracking ──


class TestOOSPercentileTracking:
    def test_with_data(self):
        result = _make_result(n_months=12, with_meta=True)
        pct = calc_oos_percentile_tracking(result)
        assert "avg_percentile" in pct
        if not np.isnan(pct["avg_percentile"]):
            assert 0 <= pct["avg_percentile"] <= 1.0

    def test_empty_result(self):
        result = WalkForwardResult([])
        pct = calc_oos_percentile_tracking(result)
        assert np.isnan(pct["avg_percentile"])

    def test_good_selection(self):
        """선정 팩터가 전체에서 상위인 경우 → 낮은 백분위."""
        result = _make_result(
            n_months=12,
            all_factor_returns={
                "F1": 0.050, "F2": 0.040, "F3": 0.030,
                "F4": -0.010, "F5": -0.020,
                "F6": -0.030, "F7": -0.040, "F8": -0.050,
                "F9": -0.015, "F10": -0.025,
            },
            active_factors=["F1", "F2", "F3"],
        )
        pct = calc_oos_percentile_tracking(result)
        if not np.isnan(pct["avg_percentile"]):
            assert pct["avg_percentile"] < 0.50


# ── 3순위: Strict Jaccard ──


class TestStrictJaccard:
    def test_identical_sets(self):
        """동일 팩터셋이면 Jaccard = 1.0."""
        history = [{"F1", "F2", "F3"}, {"F1", "F2", "F3"}, {"F1", "F2", "F3"}]
        sj = calc_strict_jaccard(history)
        assert sj["avg_jaccard"] == 1.0

    def test_disjoint_sets(self):
        """완전히 다른 팩터셋이면 Jaccard = 0.0."""
        history = [{"F1", "F2", "F3"}, {"F4", "F5", "F6"}, {"F7", "F8", "F9"}]
        sj = calc_strict_jaccard(history)
        assert sj["avg_jaccard"] == 0.0

    def test_partial_overlap(self):
        """부분 겹침."""
        history = [{"F1", "F2", "F3"}, {"F1", "F2", "F4"}]
        sj = calc_strict_jaccard(history)
        assert 0 < sj["avg_jaccard"] < 1.0
        assert abs(sj["avg_jaccard"] - 2 / 4) < 1e-10  # 2 common / 4 union = 0.5

    def test_min_samples(self):
        """리밸런싱 2회 미만이면 NaN."""
        sj = calc_strict_jaccard([{"F1"}])
        assert np.isnan(sj["avg_jaccard"])

    def test_stable_interpretation(self):
        history = [{"F1", "F2", "F3"}, {"F1", "F2", "F3"}, {"F1", "F2", "F3"}]
        sj = calc_strict_jaccard(history)
        assert "안정적" in sj["interpretation"]

    def test_unstable_interpretation(self):
        history = [{"F1", "F2", "F3"}, {"F4", "F5", "F6"}]
        sj = calc_strict_jaccard(history)
        assert "불안정" in sj["interpretation"]


# ── 4순위 (보조): IS-OOS Rank Correlation ──


class TestISoosRankCorrelation:
    def test_with_data(self):
        result = _make_result(n_months=12, with_meta=True)
        rc = calc_is_oos_rank_correlation(result)
        assert "avg_spearman" in rc

    def test_empty_result(self):
        result = WalkForwardResult([])
        rc = calc_is_oos_rank_correlation(result)
        assert np.isnan(rc["avg_spearman"])


# ── 5순위 (보조): Deflation Ratio ──


class TestDeflationRatio:
    def test_normal_case(self):
        result = _make_result()
        dr = calc_deflation_ratio(result, full_period_cagr=0.10)
        assert "deflation_ratio" in dr
        assert np.isfinite(dr["deflation_ratio"])

    def test_zero_cagr(self):
        result = _make_result()
        dr = calc_deflation_ratio(result, full_period_cagr=0.0)
        assert np.isnan(dr["deflation_ratio"])
        assert "IS CAGR = 0" in dr["interpretation"]

    def test_negative_cagr(self):
        result = _make_result()
        dr = calc_deflation_ratio(result, full_period_cagr=-0.05)
        assert np.isnan(dr["deflation_ratio"])
        assert "음수" in dr["interpretation"]
