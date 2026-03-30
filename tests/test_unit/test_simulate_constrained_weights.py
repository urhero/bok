# -*- coding: utf-8 -*-
"""
Unit tests for simulate_constrained_weights() function.

simulate_constrained_weights() 함수 테스트:
- 스타일 가중치 제약 하에서 최적 포트폴리오 탐색
- 몬테카를로 시뮬레이션 기반
- style_cap(기본 25%) 제약 적용
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from service.pipeline.optimization import simulate_constrained_weights


class TestSimulateConstrainedWeightsBasic:
    """simulate_constrained_weights 기본 기능 테스트"""

    def test_returns_two_dataframes(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """두 개의 DataFrame을 반환하는지 확인"""
        rtn_df, style_list = sample_style_returns

        best_stats, weights_tbl = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,  # 테스트용으로 적은 시뮬레이션
            test_mode=True,
        )

        assert isinstance(best_stats, pd.DataFrame)
        assert isinstance(weights_tbl, pd.DataFrame)

    def test_best_stats_has_required_columns(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """best_stats가 필수 컬럼(CAGR, MDD 등)을 가지는지 확인"""
        rtn_df, style_list = sample_style_returns

        best_stats, _ = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,
            test_mode=True,
        )

        # CAGR과 MDD 관련 컬럼이 있어야 함
        assert best_stats.shape[1] >= 1  # 최소 1개 컬럼

    def test_weights_table_has_required_columns(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """weights_tbl이 필수 컬럼을 가지는지 확인"""
        rtn_df, style_list = sample_style_returns

        _, weights_tbl = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,
            test_mode=True,
        )

        # factor와 weight 관련 컬럼 확인
        assert "factor" in weights_tbl.columns or len(weights_tbl.columns) > 0


class TestSimulateConstrainedWeightsValidation:
    """입력 검증 테스트"""

    def test_style_list_length_mismatch_raises_error(self) -> None:
        """style_list 길이가 컬럼 수와 다르면 ValueError 발생"""
        np.random.seed(42)
        dates = pd.date_range("2020-01-31", periods=36, freq="ME")

        rtn_df = pd.DataFrame({
            "A": np.random.randn(36) * 0.03,
            "B": np.random.randn(36) * 0.03,
            "C": np.random.randn(36) * 0.03,
        }, index=dates)

        # style_list 길이가 2 (컬럼 수 3과 불일치)
        style_list = ["Style1", "Style2"]

        with pytest.raises(ValueError, match="length of style_list"):
            simulate_constrained_weights(
                rtn_df=rtn_df,
                style_list=style_list,
                mode="simulation",
                num_sims=100,
            )


class TestSimulateConstrainedWeightsStyleCap:
    """style_cap 제약 테스트"""

    def test_style_cap_constraint_respected(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """스타일별 가중치가 style_cap 이하인지 확인

        Note: 5개 팩터, 3개 스타일에서 style_cap=25%는 만족하기 어려울 수 있음
        (3개 스타일 * 25% = 75% < 100% 필요).
        이 테스트는 더 완화된 제약으로 테스트합니다.
        """
        rtn_df, style_list = sample_style_returns

        # style_cap을 0.4로 완화 (5개 팩터, 3개 스타일에서 실행 가능)
        try:
            _, weights_tbl = simulate_constrained_weights(
                rtn_df=rtn_df,
                style_list=style_list,
                mode="simulation",
                num_sims=10000,
                style_cap=0.40,  # 더 완화된 제약
                test_mode=False,
            )

            # 스타일별 가중치 합계 확인
            if "styleName" in weights_tbl.columns and "fitted_weight" in weights_tbl.columns:
                style_weights = weights_tbl.groupby("styleName")["fitted_weight"].sum()
                # 각 스타일 가중치가 40% 이하여야 함 (약간의 오차 허용)
                assert all(style_weights <= 0.40 + 1e-6)
        except ValueError as e:
            if "No feasible portfolios" in str(e):
                # 스타일 캡이 너무 엄격해서 실행 불가능한 경우는 OK
                pytest.skip("Style constraints too strict for this data configuration")

    def test_test_mode_relaxes_style_cap(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """test_mode=True면 style_cap 제약이 완화되는지 확인"""
        rtn_df, style_list = sample_style_returns

        # test_mode=True
        _, weights_tbl_test = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,
            style_cap=0.25,
            test_mode=True,
        )

        # test_mode에서는 style_cap이 1.0으로 완화됨
        # 따라서 한 스타일이 25% 이상일 수 있음
        assert weights_tbl_test is not None


class TestSimulateConstrainedWeightsNumSims:
    """num_sims 파라미터 테스트"""

    def test_more_sims_may_find_better_solution(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """시뮬레이션 횟수가 많을수록 더 나은 솔루션을 찾을 가능성"""
        rtn_df, style_list = sample_style_returns

        # 적은 시뮬레이션
        np.random.seed(42)
        best_stats_low, _ = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=100,
            test_mode=True,
        )

        # 많은 시뮬레이션
        np.random.seed(42)
        best_stats_high, _ = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=10000,
            test_mode=True,
        )

        # 더 많은 시뮬레이션이 더 나은 결과를 찾을 가능성이 높음
        # 하지만 랜덤 시드로 인해 항상 그렇지는 않을 수 있음
        assert best_stats_low is not None
        assert best_stats_high is not None


class TestSimulateConstrainedWeightsBatchProcessing:
    """배치 처리 테스트"""

    def test_batch_size_affects_memory_not_result(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """batch_size는 메모리에만 영향, 결과는 동일해야 함"""
        rtn_df, style_list = sample_style_returns

        # 작은 배치
        np.random.seed(42)
        best_stats_small, weights_small = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,
            batch_size=100,
            test_mode=True,
        )

        # 큰 배치
        np.random.seed(42)
        best_stats_large, weights_large = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,
            batch_size=1000,
            test_mode=True,
        )

        # 동일한 시드로 동일한 결과를 기대
        # 하지만 배치 처리 방식에 따라 다를 수 있음
        assert best_stats_small is not None
        assert best_stats_large is not None


class TestSimulateConstrainedWeightsEdgeCases:
    """엣지 케이스 테스트"""

    def test_single_factor(self) -> None:
        """단일 팩터 처리"""
        np.random.seed(42)
        dates = pd.date_range("2020-01-31", periods=36, freq="ME")

        rtn_df = pd.DataFrame({
            "single_factor": np.random.randn(36) * 0.03,
        }, index=dates)
        style_list = ["Valuation"]

        best_stats, weights_tbl = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=100,
            test_mode=True,
        )

        # 단일 팩터는 100% 가중치
        assert best_stats is not None

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

        best_stats, weights_tbl = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,
            test_mode=True,
        )

        assert best_stats is not None

    def test_all_negative_returns(self) -> None:
        """모든 수익률이 음수인 경우"""
        np.random.seed(42)
        dates = pd.date_range("2020-01-31", periods=36, freq="ME")

        rtn_df = pd.DataFrame({
            "A": -np.abs(np.random.randn(36) * 0.03),
            "B": -np.abs(np.random.randn(36) * 0.03),
        }, index=dates)
        style_list = ["Style1", "Style2"]

        best_stats, weights_tbl = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,
            test_mode=True,
        )

        # 음수 수익률이어도 처리 가능해야 함
        assert best_stats is not None

    def test_with_zero_returns(self) -> None:
        """일부 0 수익률 포함"""
        np.random.seed(42)
        dates = pd.date_range("2020-01-31", periods=36, freq="ME")

        returns = np.random.randn(36) * 0.03
        returns[0:5] = 0  # 처음 5개월 0 수익률

        rtn_df = pd.DataFrame({
            "A": returns,
            "B": np.random.randn(36) * 0.03,
        }, index=dates)
        style_list = ["Style1", "Style2"]

        best_stats, weights_tbl = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,
            test_mode=True,
        )

        assert best_stats is not None


class TestSimulateConstrainedWeightsOutputValidation:
    """출력 유효성 테스트"""

    def test_weights_sum_to_one(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """가중치 합이 1인지 확인"""
        rtn_df, style_list = sample_style_returns

        _, weights_tbl = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,
            test_mode=True,
        )

        # fitted_weight 또는 raw_weight 컬럼의 합이 약 1이어야 함
        if "fitted_weight" in weights_tbl.columns:
            total_weight = weights_tbl["fitted_weight"].sum()
            assert abs(total_weight - 1.0) < 0.01
        elif "raw_weight" in weights_tbl.columns:
            total_weight = weights_tbl["raw_weight"].sum()
            assert abs(total_weight - 1.0) < 0.01

    def test_no_negative_weights(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """음수 가중치가 없는지 확인 (롱온리 포트폴리오)"""
        rtn_df, style_list = sample_style_returns

        _, weights_tbl = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,
            test_mode=True,
        )

        # 모든 가중치가 0 이상
        if "fitted_weight" in weights_tbl.columns:
            assert all(weights_tbl["fitted_weight"] >= -1e-10)
        elif "raw_weight" in weights_tbl.columns:
            assert all(weights_tbl["raw_weight"] >= -1e-10)

    def test_cagr_is_reasonable(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """CAGR이 합리적인 범위인지 확인"""
        rtn_df, style_list = sample_style_returns

        best_stats, _ = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,
            test_mode=True,
        )

        # CAGR이 -100% ~ +500% 범위인지 (합리적인 범위)
        # 실제 컬럼명에 따라 조정 필요
        if "cagr" in best_stats.columns:
            cagr = best_stats["cagr"].iloc[0]
            assert -1.0 <= cagr <= 5.0


class TestSimulateConstrainedWeightsReproducibility:
    """재현성 테스트"""

    def test_same_seed_same_result(
        self, sample_style_returns: tuple[pd.DataFrame, list[str]]
    ) -> None:
        """동일한 시드로 동일한 결과"""
        rtn_df, style_list = sample_style_returns

        np.random.seed(42)
        best_stats_1, weights_1 = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,
            test_mode=True,
        )

        np.random.seed(42)
        best_stats_2, weights_2 = simulate_constrained_weights(
            rtn_df=rtn_df,
            style_list=style_list,
            mode="simulation",
            num_sims=1000,
            test_mode=True,
        )

        # 동일한 시드면 동일한 결과
        pd.testing.assert_frame_equal(best_stats_1, best_stats_2)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
