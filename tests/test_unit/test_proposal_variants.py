# -*- coding: utf-8 -*-
"""2026-07 제안 실험용 게이트 파라미터 단위 테스트.

전부 기본값(off)에서 현행 동작과 동일해야 하고(회귀 가드),
켰을 때 의도한 방향으로 동작해야 한다.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from service.factor.selection import compute_rank_score, compute_tstat
from service.pipeline.factor_analysis import filter_and_label_factors
from service.pipeline.universe import resolve_hysteresis_margin, resolve_zero_cap


# ═══════════════════════════════════════════════════════════════════════════════
# Half-life 가중 t-stat
# ═══════════════════════════════════════════════════════════════════════════════

class TestHalfLifeTstat:
    def test_none_half_life_identical_to_legacy(self):
        rng = np.random.default_rng(7)
        rets = pd.DataFrame(rng.normal(0.005, 0.03, size=(60, 5)),
                            columns=list("ABCDE"))
        pd.testing.assert_series_equal(compute_tstat(rets), compute_tstat(rets, half_life=None))
        pd.testing.assert_series_equal(compute_tstat(rets), compute_tstat(rets, half_life=0))

    def test_half_life_favors_recent_performance(self):
        # 전반 60개월 음수, 후반 60개월 양수 -> 동일가중 t ~= 0, recency 가중 t > 동일가중
        old = np.full(60, -0.01)
        recent = np.full(60, 0.01)
        noise = np.random.default_rng(1).normal(0, 0.002, 120)
        rets = pd.DataFrame({"F": np.concatenate([old, recent]) + noise})
        t_eq = compute_tstat(rets)["F"]
        t_hl = compute_tstat(rets, half_life=12)["F"]
        assert t_hl > t_eq

    def test_rank_score_passes_half_life(self):
        rng = np.random.default_rng(3)
        rets = pd.DataFrame(rng.normal(0.005, 0.03, size=(60, 4)), columns=list("ABCD"))
        s_eq = compute_rank_score(rets, "tstat")
        s_hl = compute_rank_score(rets, "tstat", half_life=12)
        assert not s_eq.equals(s_hl)
        # cagr 방식은 half_life 무시
        pd.testing.assert_series_equal(
            compute_rank_score(rets, "cagr"), compute_rank_score(rets, "cagr", half_life=12)
        )

    def test_half_life_short_sample_zero(self):
        rets = pd.DataFrame({"A": [0.01]})
        assert compute_tstat(rets, half_life=12)["A"] == 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# 섹터 제거 유의성 게이트
# ═══════════════════════════════════════════════════════════════════════════════

def _sector_factor_data(monthly_spreads: dict[str, list[float]]):
    """섹터별 월간 Q1-Q5 스프레드를 지정해 (sector_return_df, raw_df) 튜플 생성.

    Q3 = 0 고정, Q1 = +spread/2, Q5 = -spread/2 로 배치 (결정적, 노이즈 없음).
    """
    n_months = len(next(iter(monthly_spreads.values())))
    dates = pd.date_range("2024-01-31", periods=n_months, freq="ME")
    rows = []
    sector_means = {}
    for sec, spreads in monthly_spreads.items():
        for di, (date, sp) in enumerate(zip(dates, spreads)):
            q_ret = {"Q1": sp / 2, "Q2": sp / 4, "Q3": 0.0, "Q4": -sp / 4, "Q5": -sp / 2}
            for q, r in q_ret.items():
                for j in range(3):
                    rows.append({
                        "gvkeyiid": f"GV{sec}{q}{j}", "ticker": f"{sec}_{q}_{j}",
                        "isin": f"KR{sec}{q}{j:03d}", "ddt": date, "sec": sec,
                        "quantile": q, "M_RETURN": r,
                    })
        mean_sp = float(np.mean(spreads))
        sector_means[sec] = (mean_sp / 2, mean_sp / 4, 0.0, -mean_sp / 4, -mean_sp / 2)
    sector_ret = pd.DataFrame(sector_means, index=["Q1", "Q2", "Q3", "Q4", "Q5"])
    sector_ret.columns.name = "sec"
    return sector_ret, None, None, pd.DataFrame(rows)


class TestSectorDropTstatGate:
    def test_insignificant_negative_sector_kept_with_gate(self):
        data = _sector_factor_data({
            "Good": [0.06, 0.05, 0.07, 0.06, 0.05],          # 확실한 양의 스프레드
            "Noisy": [-0.05, 0.04, -0.02, 0.03, -0.03],       # 평균 -0.006, |t| < 1
        })
        # 게이트 off (현행): 평균 음수 -> 제거
        _, _, _, _, dropped_off, _ = filter_and_label_factors(
            ["F1"], ["Factor1"], ["Value"], [data])
        assert "Noisy" in dropped_off[0]
        # 게이트 on: 유의하지 않으므로 유지
        _, _, _, _, dropped_on, _ = filter_and_label_factors(
            ["F1"], ["Factor1"], ["Value"], [data], sector_drop_tstat=1.0)
        assert "Noisy" not in dropped_on[0]

    def test_significant_negative_sector_still_dropped(self):
        data = _sector_factor_data({
            "Good": [0.06, 0.05, 0.07, 0.06, 0.05],
            "Bad": [-0.05, -0.04, -0.06, -0.05, -0.045],      # 일관된 음수 -> t << -1
        })
        _, _, _, _, dropped_on, _ = filter_and_label_factors(
            ["F1"], ["Factor1"], ["Value"], [data], sector_drop_tstat=1.0)
        assert "Bad" in dropped_on[0]

    def test_gate_off_matches_legacy(self):
        data = _sector_factor_data({
            "Good": [0.06, 0.05, 0.07, 0.06, 0.05],
            "Bad": [-0.05, -0.04, -0.06, -0.05, -0.045],
        })
        legacy = filter_and_label_factors(["F1"], ["Factor1"], ["Value"], [data])
        explicit = filter_and_label_factors(["F1"], ["Factor1"], ["Value"], [data],
                                            sector_drop_tstat=None)
        assert legacy[4] == explicit[4]
        pd.testing.assert_frame_equal(legacy[5][0], explicit[5][0])


# ═══════════════════════════════════════════════════════════════════════════════
# resolve_zero_cap / resolve_hysteresis_margin
# ═══════════════════════════════════════════════════════════════════════════════

class TestResolveZeroCap:
    def test_default_fixed(self):
        pp = {"max_zero_return_months": 10}
        assert resolve_zero_cap(pp, 36) == 10
        assert resolve_zero_cap(pp, 190) == 10

    def test_frac_proportional(self):
        pp = {"max_zero_return_months": 10, "max_zero_return_frac": 0.05}
        assert resolve_zero_cap(pp, 200) == 10
        assert resolve_zero_cap(pp, 40) == 2
        assert resolve_zero_cap(pp, 10) == 1  # 최소 1


class TestResolveHysteresisMargin:
    def test_absolute_default(self):
        scores = pd.Series([0.0, 1.0, 2.0, 3.0])
        assert resolve_hysteresis_margin({}, 0.5, scores) == 0.5

    def test_iqr_mode_scales(self):
        scores = pd.Series([0.0, 1.0, 2.0, 3.0])  # IQR = 1.5
        pp = {"hysteresis_margin_mode": "iqr"}
        assert resolve_hysteresis_margin(pp, 0.5, scores) == pytest.approx(0.75)

    def test_iqr_degenerate_falls_back(self):
        # IQR=0 (동일 점수) -> 절대값 fallback
        scores = pd.Series([1.0, 1.0, 1.0])
        pp = {"hysteresis_margin_mode": "iqr"}
        assert resolve_hysteresis_margin(pp, 0.5, scores) == 0.5
