# -*- coding: utf-8 -*-
"""벤치마크 비교 모듈 단위 테스트."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from service.pipeline.benchmark_comparison import (
    compare_vs_benchmark,
    create_equal_weight_benchmark,
    create_mp_portfolio_return,
)


@pytest.fixture
def sample_ret_df():
    """테스트용 팩터 수익률 행렬 (첫 행 0)."""
    dates = pd.date_range("2020-01-31", periods=13, freq="ME")
    np.random.seed(42)
    df = pd.DataFrame({
        "FactorA": np.random.randn(13) * 0.03,
        "FactorB": np.random.randn(13) * 0.02,
        "FactorC": np.random.randn(13) * 0.025,
    }, index=dates)
    df.iloc[0] = 0.0
    return df


class TestEqualWeightBenchmark:
    def test_return_series_shape(self, sample_ret_df):
        result = create_equal_weight_benchmark(sample_ret_df)
        assert len(result["return_series"]) == len(sample_ret_df)

    def test_first_row_zero(self, sample_ret_df):
        result = create_equal_weight_benchmark(sample_ret_df)
        assert result["return_series"].iloc[0] == 0.0

    def test_cagr_is_finite(self, sample_ret_df):
        result = create_equal_weight_benchmark(sample_ret_df)
        assert np.isfinite(result["cagr"])

    def test_mdd_is_nonpositive(self, sample_ret_df):
        result = create_equal_weight_benchmark(sample_ret_df)
        assert result["mdd"] <= 0


class TestMPPortfolioReturn:
    def test_weighted_return(self, sample_ret_df):
        weights = {"FactorA": 0.5, "FactorB": 0.3, "FactorC": 0.2}
        result = create_mp_portfolio_return(sample_ret_df, weights)
        assert len(result["return_series"]) == len(sample_ret_df)

    def test_no_matching_factors_raises(self, sample_ret_df):
        with pytest.raises(ValueError):
            create_mp_portfolio_return(sample_ret_df, {"NonExistent": 1.0})

    def test_cagr_formula_consistency(self, sample_ret_df):
        """파이프라인과 동일한 CAGR 공식 사용 확인."""
        weights = {"FactorA": 0.5, "FactorB": 0.3, "FactorC": 0.2}
        result = create_mp_portfolio_return(sample_ret_df, weights)
        # 수동 계산
        w = pd.Series(weights)
        manual_ret = (sample_ret_df[w.index] * w).sum(axis=1)
        manual_cum = (1 + manual_ret).cumprod()
        months = len(sample_ret_df) - 1
        manual_cagr = manual_cum.iloc[-1] ** (12 / months) - 1
        assert abs(result["cagr"] - manual_cagr) < 1e-10


class TestCompareVsBenchmark:
    def test_report_keys(self, sample_ret_df):
        weights = {"FactorA": 0.5, "FactorB": 0.3, "FactorC": 0.2}
        report = compare_vs_benchmark(sample_ret_df, weights)
        expected_keys = {"mp_cagr", "ew_cagr", "excess_cagr", "mp_mdd", "ew_mdd",
                         "mp_sharpe", "ew_sharpe", "win_rate", "t_statistic", "p_value"}
        assert expected_keys.issubset(report.keys())

    def test_zero_excess_return_case(self):
        """모든 팩터가 동일할 때 excess_cagr ≈ 0."""
        dates = pd.date_range("2020-01-31", periods=13, freq="ME")
        ret = [0.0] + [0.01] * 12
        df = pd.DataFrame({"A": ret, "B": ret, "C": ret}, index=dates)
        weights = {"A": 1 / 3, "B": 1 / 3, "C": 1 / 3}
        report = compare_vs_benchmark(df, weights)
        assert abs(report["excess_cagr"]) < 1e-10

    def test_ttest_with_known_data(self):
        """알려진 초과수익 데이터에서 t-test가 정상 동작하는지 확인."""
        dates = pd.date_range("2020-01-31", periods=25, freq="ME")
        # FactorA가 일관적으로 FactorB보다 높은 수익
        np.random.seed(42)
        df = pd.DataFrame({
            "A": [0.0] + list(np.random.randn(24) * 0.01 + 0.02),
            "B": [0.0] + list(np.random.randn(24) * 0.01),
        }, index=dates)
        weights = {"A": 1.0, "B": 0.0}
        report = compare_vs_benchmark(df, weights)
        # A에 100% 가중 → EW보다 높을 것
        assert report["t_statistic"] > 0 or report["excess_cagr"] > 0
