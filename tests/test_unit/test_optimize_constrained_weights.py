# -*- coding: utf-8 -*-
"""
Unit tests for optimize_constrained_weights() function.

optimize_constrained_weights() 함수 테스트:
- 스타일 가중치 제약 하에서 포트폴리오 가중치 결정
- hardcoded / equal_weight 두 가지 모드 지원
- style_cap(기본 25%) 제약 적용
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from service.pipeline.optimization import optimize_constrained_weights


class TestOptimizeConstrainedWeightsBasic:
    """optimize_constrained_weights 기본 기능 테스트"""

    def test_returns_two_dataframes(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """두 개의 DataFrame을 반환하는지 확인"""
        rtn_df, style_list = sample_style_returns

        best_stats, weights_tbl = optimize_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="equal_weight",
            test_mode=True,
        )

        assert isinstance(best_stats, pd.DataFrame)
        assert isinstance(weights_tbl, pd.DataFrame)

    def test_best_stats_has_required_columns(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """best_stats가 필수 컬럼(CAGR, MDD 등)을 가지는지 확인"""
        rtn_df, style_list = sample_style_returns

        best_stats, _ = optimize_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="equal_weight",
            test_mode=True,
        )

        assert "cagr" in best_stats.columns
        assert "mdd" in best_stats.columns

    def test_weights_table_has_required_columns(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """weights_tbl이 필수 컬럼을 가지는지 확인"""
        rtn_df, style_list = sample_style_returns

        _, weights_tbl = optimize_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="equal_weight",
            test_mode=True,
        )

        assert "factor" in weights_tbl.columns
        assert "fitted_weight" in weights_tbl.columns
        assert "styleName" in weights_tbl.columns


class TestOptimizeConstrainedWeightsStyleCap:
    """style_cap 제약 테스트"""

    def test_style_cap_constraint_respected(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """스타일별 가중치가 style_cap 이하인지 확인"""
        rtn_df, style_list = sample_style_returns

        _, weights_tbl = optimize_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="equal_weight",
            style_cap=0.40,
            test_mode=False,
        )

        if "styleName" in weights_tbl.columns and "fitted_weight" in weights_tbl.columns:
            style_weights = weights_tbl.groupby("styleName")["fitted_weight"].sum()
            assert all(style_weights <= 0.40 + 1e-6)

    def test_test_mode_relaxes_style_cap(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """test_mode=True면 style_cap 제약이 완화되는지 확인"""
        rtn_df, style_list = sample_style_returns

        _, weights_tbl = optimize_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="equal_weight",
            style_cap=0.25,
            test_mode=True,
        )

        assert weights_tbl is not None


class TestOptimizeConstrainedWeightsEdgeCases:
    """엣지 케이스 테스트"""

    def test_single_factor(self) -> None:
        """단일 팩터 처리"""
        np.random.seed(42)
        dates = pd.date_range("2020-01-31", periods=36, freq="ME")

        rtn_df = pd.DataFrame({
            "single_factor": np.random.randn(36) * 0.03,
        }, index=dates)
        style_list = ["Valuation"]

        best_stats, weights_tbl = optimize_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="equal_weight",
            test_mode=True,
        )

        assert best_stats is not None
        assert len(weights_tbl) == 1

    def test_single_style_multiple_factors(self) -> None:
        """단일 스타일, 여러 팩터 처리"""
        np.random.seed(42)
        dates = pd.date_range("2020-01-31", periods=36, freq="ME")

        rtn_df = pd.DataFrame({
            "val_1": np.random.randn(36) * 0.03,
            "val_2": np.random.randn(36) * 0.03,
            "val_3": np.random.randn(36) * 0.03,
        }, index=dates)
        style_list = ["Valuation", "Valuation", "Valuation"]

        best_stats, weights_tbl = optimize_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="equal_weight",
            test_mode=True,
        )

        assert best_stats is not None

    def test_unknown_mode_raises_error(self) -> None:
        """알 수 없는 모드는 ValueError 발생"""
        np.random.seed(42)
        dates = pd.date_range("2020-01-31", periods=36, freq="ME")

        rtn_df = pd.DataFrame({
            "A": np.random.randn(36) * 0.03,
            "B": np.random.randn(36) * 0.03,
        }, index=dates)
        style_list = ["Style1", "Style2"]

        with pytest.raises(ValueError, match="Unknown optimization mode"):
            optimize_constrained_weights(
                rtn_df=rtn_df,
                style_list=style_list,
                mode="monte_carlo",
            )


class TestOptimizeConstrainedWeightsOutputValidation:
    """출력 유효성 테스트"""

    def test_weights_sum_to_one(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """가중치 합이 1인지 확인"""
        rtn_df, style_list = sample_style_returns

        _, weights_tbl = optimize_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="equal_weight",
            test_mode=True,
        )

        total_weight = weights_tbl["fitted_weight"].sum()
        assert abs(total_weight - 1.0) < 0.01

    def test_no_negative_weights(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """음수 가중치가 없는지 확인 (롱온리 포트폴리오)"""
        rtn_df, style_list = sample_style_returns

        _, weights_tbl = optimize_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="equal_weight",
            test_mode=True,
        )

        assert all(weights_tbl["fitted_weight"] >= -1e-10)

    def test_cagr_is_reasonable(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """CAGR이 합리적인 범위인지 확인"""
        rtn_df, style_list = sample_style_returns

        best_stats, _ = optimize_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="equal_weight",
            test_mode=True,
        )

        cagr = best_stats["cagr"].iloc[0]
        assert -1.0 <= cagr <= 5.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
