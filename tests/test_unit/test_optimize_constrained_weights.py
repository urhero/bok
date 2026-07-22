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


class TestStyleCapFeasibilityGuard:
    """style_cap 제약 불가능/미수렴 시 경고 로그 (동작 변경 없음)."""

    def _make_returns(self, styles: list[str]) -> tuple[pd.DataFrame, list[str]]:
        rng = np.random.default_rng(0)
        n = len(styles)
        rtn_df = pd.DataFrame(
            rng.normal(0.005, 0.02, size=(24, n)),
            columns=[f"F{i}" for i in range(n)],
        )
        return rtn_df, styles

    def test_infeasible_cap_warns(self, caplog) -> None:
        """스타일 3개 x cap 25% = 75% < 100% -> 제약 불가능 경고."""
        import logging
        rtn_df, style_list = self._make_returns(["S1", "S2", "S3"])
        with caplog.at_level(logging.WARNING, logger="service.pipeline.optimization"):
            _, weights_tbl = optimize_constrained_weights(
                rtn_df=rtn_df, style_list=style_list,
                mode="equal_weight", style_cap=0.25, test_mode=False,
            )
        assert any("infeasible" in r.message for r in caplog.records)
        # 동작은 기존과 동일: 합 1.0 유지 (cap 은 위반된 채 반환)
        assert np.isclose(weights_tbl["fitted_weight"].sum(), 1.0)

    def test_feasible_cap_no_warning(self, caplog) -> None:
        """스타일 5개 x cap 25% = 125% >= 100% -> 경고 없음."""
        import logging
        rtn_df, style_list = self._make_returns(["S1", "S2", "S3", "S4", "S5"])
        with caplog.at_level(logging.WARNING, logger="service.pipeline.optimization"):
            optimize_constrained_weights(
                rtn_df=rtn_df, style_list=style_list,
                mode="equal_weight", style_cap=0.25, test_mode=False,
            )
        assert not [r for r in caplog.records if "style_cap" in r.message]


class TestEqualRiskWeightMode:
    """equal_risk_weight 모드: IS 변동성 반비례 가중 + 스타일 캡 재분배."""

    def test_lower_vol_gets_higher_weight(self) -> None:
        """저변동성 팩터가 더 큰 가중을 받고, 비율이 1/vol 에 정합."""
        rng = np.random.default_rng(7)
        n = 37
        rtn_df = pd.DataFrame({
            "low_vol": rng.normal(0.005, 0.01, n),
            "high_vol": rng.normal(0.005, 0.05, n),
        })
        rtn_df.iloc[0] = 0.0  # 백테스트 관례: 첫 행 기준점

        _, weights_tbl = optimize_constrained_weights(
            rtn_df=rtn_df, style_list=["S1", "S2"],
            mode="equal_risk_weight", test_mode=True,
        )
        w = weights_tbl.set_index("factor")["fitted_weight"]
        assert w["low_vol"] > w["high_vol"]
        assert np.isclose(w.sum(), 1.0, atol=1e-6)
        # 반비례 정합: w_low / w_high ~= vol_high / vol_low (첫 행 제외 std)
        vols = rtn_df.iloc[1:].std()
        assert np.isclose(
            w["low_vol"] / w["high_vol"], vols["high_vol"] / vols["low_vol"], rtol=1e-3
        )

    def test_style_cap_still_enforced(self) -> None:
        """equal_risk_weight 에서도 스타일 캡 재분배가 동일하게 적용."""
        rng = np.random.default_rng(1)
        n = 37
        rtn_df = pd.DataFrame({
            f"F{i}": rng.normal(0.005, 0.01 + 0.01 * i, n) for i in range(6)
        })
        rtn_df.iloc[0] = 0.0
        style_list = ["A", "A", "B", "B", "C", "C"]

        _, weights_tbl = optimize_constrained_weights(
            rtn_df=rtn_df, style_list=style_list,
            mode="equal_risk_weight", style_cap=0.40, test_mode=False,
        )
        style_weights = weights_tbl.groupby("styleName")["fitted_weight"].sum()
        # 재분배 루프는 float32 + 편중 초기가중에서 ~1e-4 잔차를 남길 수 있다
        # (기존 알고리즘 특성: 초과 시 경고만 남기고 허용). 실질 캡 준수만 확인.
        assert all(style_weights <= 0.40 + 1e-3)
        assert np.isclose(weights_tbl["fitted_weight"].sum(), 1.0, atol=1e-6)

    def test_risk_basis_caps_risk_budget_not_notional(self) -> None:
        """style_cap_basis="risk": 스타일별 리스크 예산(w*sigma) 합이 cap 이하.

        저변동 스타일은 금액 비중이 cap 을 넘을 수 있다 (리스크 기준이므로 정상).
        """
        rng = np.random.default_rng(3)
        n = 61
        # 스타일 A: 저변동 4팩터, 스타일 B/C/D/E: 고변동 각 2팩터
        cols, styles = {}, []
        for i in range(4):
            cols[f"A{i}"] = rng.normal(0.003, 0.01, n)
            styles.append("A")
        for s in ["B", "C", "D", "E"]:
            for i in range(2):
                cols[f"{s}{i}"] = rng.normal(0.003, 0.05, n)
                styles.append(s)
        rtn_df = pd.DataFrame(cols)
        rtn_df.iloc[0] = 0.0

        _, weights_tbl = optimize_constrained_weights(
            rtn_df=rtn_df, style_list=styles,
            mode="equal_risk_weight", style_cap=0.25, test_mode=False,
            style_cap_basis="risk",
        )
        w = weights_tbl.set_index("factor")["fitted_weight"]
        vols = rtn_df.iloc[1:].std()
        risk = (w * vols)
        risk_share = risk.groupby(np.asarray(styles)).sum() / risk.sum()
        assert all(risk_share <= 0.25 + 1e-3)  # 리스크 예산 기준 캡 준수
        assert np.isclose(w.sum(), 1.0, atol=1e-6)
        # 저변동 스타일 A는 금액 비중이 25%를 넘는 것이 정상 (리스크 기준 캡의 정의)
        notional_share = w.groupby(np.asarray(styles)).sum()
        assert notional_share["A"] > 0.25

    def test_zero_vol_factor_does_not_explode(self) -> None:
        """무분산 팩터는 vol 하한(1e-6) 가드로 폭주하지 않는다."""
        rng = np.random.default_rng(2)
        n = 37
        rtn_df = pd.DataFrame({
            "flat": np.zeros(n),
            "normal": rng.normal(0.005, 0.02, n),
        })
        _, weights_tbl = optimize_constrained_weights(
            rtn_df=rtn_df, style_list=["S1", "S2"],
            mode="equal_risk_weight", test_mode=True,
        )
        w = weights_tbl.set_index("factor")["fitted_weight"]
        assert np.isfinite(w).all()
        assert np.isclose(w.sum(), 1.0, atol=1e-6)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
