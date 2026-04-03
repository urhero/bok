# -*- coding: utf-8 -*-
"""과적합 진단 모듈 단위 테스트."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from service.backtest.overfit_diagnostics import (
    calc_deflation_ratio,
    calc_factor_selection_stability,
    calc_is_oos_rank_correlation,
)
from service.backtest.result_stitcher import WalkForwardResult


def _make_result(n_months=12, mp_ret=0.01, ew_ret=0.005, with_meta=True):
    """간단한 WalkForwardResult 생성 헬퍼."""
    dates = pd.date_range("2021-01-31", periods=n_months, freq="ME")
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
        }
        if with_meta and i % 3 == 0:
            entry["is_meta"] = pd.DataFrame({
                "factorAbbreviation": ["F1", "F2", "F3"],
                "cagr": [0.15, 0.10, 0.05],
                "styleName": ["Val", "Mom", "Qual"],
            })
        results.append(entry)
    return WalkForwardResult(results)


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


class TestFactorSelectionStability:
    def test_normal_case(self):
        result = _make_result(n_months=12, with_meta=True)
        stability = calc_factor_selection_stability(result.is_meta_history)
        assert "avg_jaccard" in stability
        if len(result.is_meta_history) >= 2:
            assert np.isfinite(stability["avg_jaccard"])

    def test_min_samples(self):
        """Tier 2 리밸런싱 2회 미만 시 NaN."""
        result = _make_result(n_months=2, with_meta=True)
        stability = calc_factor_selection_stability(result.is_meta_history)
        if len(result.is_meta_history) < 2:
            assert np.isnan(stability["avg_jaccard"])

    def test_identical_sets(self):
        """동일 팩터셋이면 Jaccard = 1.0."""
        meta = pd.DataFrame({"factorAbbreviation": ["F1", "F2", "F3"], "cagr": [0.1, 0.05, 0.02]})
        stability = calc_factor_selection_stability([meta, meta, meta])
        assert stability["avg_jaccard"] == 1.0


class TestISoosRankCorrelation:
    def test_with_data(self):
        result = _make_result(n_months=12, with_meta=True)
        rc = calc_is_oos_rank_correlation(result)
        assert "avg_spearman" in rc

    def test_empty_result(self):
        result = WalkForwardResult([])
        rc = calc_is_oos_rank_correlation(result)
        assert np.isnan(rc["avg_spearman"])
