# -*- coding: utf-8 -*-
"""종목별 BM 비중 숏 상한 (2026-08-21)."""
import pandas as pd
import pytest

from service.pipeline.bm_weights import apply_bm_short_cap


def test_short_capped_at_bm_weight():
    """BM 비중을 넘는 숏은 잘린다."""
    w = pd.Series({"A": 0.10, "S1": -0.08, "S2": -0.02})
    bm = pd.Series({"A": 0.05, "S1": 0.03, "S2": 0.50})
    out = apply_bm_short_cap(w, bm)
    assert out["S1"] >= -bm["S1"] - 1e-12


def test_dollar_neutrality_preserved():
    """롱은 숏 총액에 맞춰 비례 축소 -> 항상 중립."""
    w = pd.Series({"A": 0.06, "B": 0.04, "S1": -0.08, "S2": -0.02})
    bm = pd.Series({"A": 0.05, "B": 0.05, "S1": 0.03, "S2": 0.05})
    out = apply_bm_short_cap(w, bm)
    assert out[out > 0].sum() == pytest.approx(-out[out < 0].sum())
    assert out["A"] / out["B"] == pytest.approx(0.06 / 0.04)   # 롱 상대비율 보존
    assert out["S1"] == pytest.approx(-0.03)                   # 상한, 재분배 없음


def test_target_shortfall_is_accepted():
    """상한으로 숏이 줄면 목표 노출에 미달한 채로 둔다 (억지로 채우지 않음)."""
    w = pd.Series({"A": 0.10, "S1": -0.08, "S2": -0.02})
    bm = pd.Series({"A": 0.05, "S1": 0.03, "S2": 0.05})
    out = apply_bm_short_cap(w, bm)
    assert -out[out < 0].sum() == pytest.approx(0.05)   # 0.03(상한) + 0.02(원본)
    assert out[out > 0].sum() == pytest.approx(0.05)    # 롱도 동일하게 축소


def test_not_in_bm_cannot_be_shorted():
    """BM 미편입(비중 0) 종목은 숏 한도 0 -> 전액 제거."""
    w = pd.Series({"A": 0.05, "S1": -0.03, "GHOST": -0.02})
    bm = pd.Series({"A": 0.05, "S1": 0.10})     # GHOST 없음
    out = apply_bm_short_cap(w, bm)
    assert out["GHOST"] == pytest.approx(0.0)
    assert out["S1"] == pytest.approx(-0.03)           # 원본 유지 (재분배 없음)


def test_no_shorts_is_noop():
    w = pd.Series({"A": 0.05, "B": 0.05})
    pd.testing.assert_series_equal(apply_bm_short_cap(w, pd.Series({"A": 0.1, "B": 0.1})), w)


def test_excess_is_cut_not_redistributed():
    """초과분은 잘라내기만 — 다른 숏으로 밀지 않는다.

    재분배(water-filling)하면 소형주(BM 비중 작음) 숏이 대형주 숏으로 이동해 팩터
    신호가 사이즈 베팅으로 변질된다 (2026-08-21 실측: Sharpe +0.715 -> -0.855) -> 폐기.
    """
    w = pd.Series({"A": 0.10, "S1": -0.08, "S2": -0.02})
    bm = pd.Series({"A": 0.05, "S1": 0.03, "S2": 0.50})
    out = apply_bm_short_cap(w, bm)
    assert out["S1"] == pytest.approx(-0.03)               # 상한까지만
    assert out["S2"] == pytest.approx(-0.02)               # 원본 유지 (재분배 없음)
    assert out[out > 0].sum() == pytest.approx(0.05)       # 롱도 0.05 로 축소 (중립)


def test_relative_short_weights_preserved():
    """상한 안 걸린 종목끼리의 상대 비중이 보존된다 (팩터 신호 유지)."""
    w = pd.Series({"A": 0.10, "BIG": -0.06, "S1": -0.03, "S2": -0.01})
    bm = pd.Series({"A": 0.05, "BIG": 0.02, "S1": 0.50, "S2": 0.50})
    out = apply_bm_short_cap(w, bm)
    assert out["BIG"] == pytest.approx(-0.02)
    assert out["S1"] / out["S2"] == pytest.approx(3.0)     # -0.03 : -0.01 유지
