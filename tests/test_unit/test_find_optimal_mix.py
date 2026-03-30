# -*- coding: utf-8 -*-
"""find_optimal_mix 함수 유닛 테스트.

2-팩터 믹스 그리드 탐색, CAGR/MDD 계산, rank_total 컬럼을 검증한다.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from service.pipeline.optimization import find_optimal_mix


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def factor_returns_5() -> pd.DataFrame:
    """5개 팩터의 월간 수익률 행렬 (첫 행 = 0 기준점)."""
    np.random.seed(42)
    n_months = 37  # 첫 행 0 포함하여 36개월 + 1
    dates = pd.date_range("2021-01-31", periods=n_months, freq="ME")

    data = {}
    for name in ["FactorA", "FactorB", "FactorC", "FactorD", "FactorE"]:
        rets = np.random.randn(n_months) * 0.03
        rets[0] = 0.0  # 기준점
        data[name] = rets

    return pd.DataFrame(data, index=dates)


@pytest.fixture
def main_factor_meta() -> pd.DataFrame:
    """메인 팩터 메타데이터 (1행 DataFrame)."""
    return pd.DataFrame({
        "factorAbbreviation": ["FactorA"],
        "styleName": ["Valuation"],
        "cagr": [0.12],
    })


@pytest.fixture
def downside_corr_matrix(factor_returns_5) -> pd.DataFrame:
    """하락 상관관계 행렬 (CAGR 순 정렬됨)."""
    cols = factor_returns_5.columns.tolist()
    np.random.seed(123)
    n = len(cols)
    # 대칭 행렬 생성 (-1 ~ 1 범위)
    mat = np.random.randn(n, n) * 0.3
    mat = (mat + mat.T) / 2
    np.fill_diagonal(mat, 1.0)
    return pd.DataFrame(mat, index=cols, columns=cols)


# ═══════════════════════════════════════════════════════════════════════════════
# Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestFindOptimalMixBasic:
    """기본 기능 테스트."""

    def test_returns_dataframe(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """DataFrame을 반환하는지 확인."""
        result = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        assert isinstance(result, pd.DataFrame)

    def test_expected_columns_exist(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """필수 컬럼이 존재하는지 확인."""
        df = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        expected_cols = {
            "main_wgt", "sub_wgt", "mix_cagr", "mix_mdd",
            "main_factor", "sub_factor", "rank_total",
            "main_cagr", "sub_cagr", "main_mdd", "sub_mdd",
        }
        assert expected_cols.issubset(set(df.columns))

    def test_main_factor_name_consistent(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """main_factor 컬럼이 메인 팩터명과 일치하는지 확인."""
        df = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        assert (df["main_factor"] == "FactorA").all()


class TestFindOptimalMixGrid:
    """그리드 탐색 테스트."""

    def test_grid_has_101_points_per_sub(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """보조 팩터당 101개 그리드 포인트가 생성되는지 확인."""
        df = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        # 각 sub_factor별 행 수가 101인지 확인
        for sub in df["sub_factor"].unique():
            sub_df = df[df["sub_factor"] == sub]
            assert len(sub_df) == 101, f"Sub-factor {sub} has {len(sub_df)} rows, expected 101"

    def test_main_wgt_range_0_to_1(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """main_wgt가 0~1 범위인지 확인."""
        df = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        assert df["main_wgt"].min() >= 0.0 - 1e-10
        assert df["main_wgt"].max() <= 1.0 + 1e-10

    def test_weights_sum_to_one(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """main_wgt + sub_wgt = 1 인지 확인."""
        df = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        weight_sums = df["main_wgt"] + df["sub_wgt"]
        np.testing.assert_array_almost_equal(weight_sums.values, 1.0, decimal=10)


class TestFindOptimalMixSkipSelf:
    """자기 자신과의 믹스 스킵 테스트."""

    def test_main_not_in_sub_factors(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """main_factor와 동일한 sub_factor가 결과에 없는지 확인."""
        df = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        assert "FactorA" not in df["sub_factor"].values


class TestFindOptimalMixMetrics:
    """CAGR 및 MDD 계산 테스트."""

    def test_cagr_is_finite(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """mix_cagr 값이 유한한지 확인."""
        df = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        assert np.isfinite(df["mix_cagr"]).all()

    def test_mdd_is_non_positive(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """mix_mdd 값이 0 이하인지 확인 (MDD는 항상 음수 또는 0)."""
        df = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        assert (df["mix_mdd"] <= 1e-10).all()

    def test_rank_total_exists_and_positive(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """rank_total 컬럼이 존재하고 양수인지 확인."""
        df = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        assert "rank_total" in df.columns
        assert (df["rank_total"] > 0).all()

    def test_pure_main_cagr_matches(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """main_wgt=1.0일 때 mix_cagr == main_cagr인지 확인."""
        df = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        pure_main = df[np.isclose(df["main_wgt"], 1.0)]
        if not pure_main.empty:
            np.testing.assert_array_almost_equal(
                pure_main["mix_cagr"].values,
                pure_main["main_cagr"].values,
                decimal=6,
            )

    def test_pure_sub_cagr_matches(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """main_wgt=0.0일 때 mix_cagr == sub_cagr인지 확인."""
        df = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        pure_sub = df[np.isclose(df["main_wgt"], 0.0)]
        if not pure_sub.empty:
            np.testing.assert_array_almost_equal(
                pure_sub["mix_cagr"].values,
                pure_sub["sub_cagr"].values,
                decimal=6,
            )


class TestFindOptimalMixSubFactorCount:
    """보조 팩터 후보 수 테스트."""

    def test_max_3_sub_factors(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """보조 팩터가 최대 3개인지 확인."""
        df = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        n_subs = df["sub_factor"].nunique()
        assert n_subs <= 3

    def test_at_least_one_sub_factor(self, factor_returns_5, main_factor_meta, downside_corr_matrix):
        """최소 1개의 보조 팩터가 존재하는지 확인."""
        df = find_optimal_mix(factor_returns_5, main_factor_meta, downside_corr_matrix)
        assert df["sub_factor"].nunique() >= 1
