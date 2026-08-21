# -*- coding: utf-8 -*-
"""종목단 배포 기준 시계열 (2026-08-19)."""
import numpy as np
import pandas as pd
import pytest

from service.backtest.stock_level import build_stock_series, series_metrics


def _monthly(gross_list, ret=0.01):
    """gross 가 달라지는 2종목 북 시퀀스를 만든다 (롱1 / 숏1)."""
    out = []
    for i, g in enumerate(gross_list):
        w = pd.Series({"A": g / 2, "B": -g / 2})
        r = pd.Series({"A": ret, "B": -ret})
        out.append((pd.Timestamp("2020-01-31") + pd.DateOffset(months=i), w, r))
    return out


def test_target_gross_fixes_exposure_every_month():
    """netting 이 달라도 매월 노출이 목표값으로 고정된다."""
    df = build_stock_series(_monthly([0.86, 0.90, 0.70]), 10.0, 0.40)
    assert df["long_exposure"].tolist() == pytest.approx([0.20, 0.20, 0.20])
    assert df["short_exposure"].tolist() == pytest.approx([-0.20, -0.20, -0.20])
    # 배수는 gross 에 반비례
    assert df["multiplier"].tolist() == pytest.approx([0.40 / 0.86, 0.40 / 0.90, 0.40 / 0.70])


def test_no_target_leaves_book_unscaled():
    df = build_stock_series(_monthly([0.86, 0.90]), 10.0, None)
    assert (df["multiplier"] == 1.0).all()
    assert df["long_exposure"].tolist() == pytest.approx([0.43, 0.45])


def test_returns_scale_with_multiplier():
    """수익·비용은 배수에 거의 선형 — NAV 드리프트만 2차 항으로 남는다.

    비중 변화(턴오버)를 실제 배포 북의 NAV 로 정규화하므로 완전한 상수배는
    아니다 (드리프트가 스케일에 비선형). 차이는 상대 1e-4 수준.
    """
    un = build_stock_series(_monthly([0.80, 0.80]), 10.0, None)
    sc = build_stock_series(_monthly([0.80, 0.80]), 10.0, 0.40)
    ratio = 0.40 / 0.80
    assert sc["gross_return"].tolist() == pytest.approx((un["gross_return"] * ratio).tolist())
    assert sc["net_return"].tolist() == pytest.approx(
        (un["net_return"] * ratio).tolist(), rel=1e-4)


def test_tracking_error_is_annualized_std():
    """시장중립 오버레이 -> 액티브수익=오버레이수익 -> TE = 월수익 std x sqrt(12)."""
    df = build_stock_series(_monthly([0.80] * 12), 10.0, 0.40)
    m = series_metrics(df)
    r = df["net_return"].dropna()
    assert m["tracking_error"] == pytest.approx(r.std() * np.sqrt(12))


def _varied(gross_list, rets):
    """월별 수익이 다른 시퀀스 (Sharpe 가 퇴화하지 않도록)."""
    out = []
    for i, (g, r) in enumerate(zip(gross_list, rets)):
        w = pd.Series({"A": g / 2, "B": -g / 2})
        rr = pd.Series({"A": r, "B": -r})
        out.append((pd.Timestamp("2020-01-31") + pd.DateOffset(months=i), w, rr))
    return out


def test_sharpe_invariant_only_when_gross_constant():
    """gross 가 일정하면 배수는 상수배 -> Sharpe 불변."""
    seq = _varied([0.80] * 6, [0.02, -0.01, 0.03, 0.005, -0.02, 0.015])
    m_un = series_metrics(build_stock_series(seq, 10.0, None))
    m_sc = series_metrics(build_stock_series(seq, 10.0, 0.40))
    assert m_sc["sharpe"] == pytest.approx(m_un["sharpe"], rel=1e-3)


def test_targeting_changes_return_path_when_gross_varies():
    """gross 가 변하면 목표 노출 고정은 '상수배'가 아니라 월별 가중을 바꾼다.

    netting 이 심한 달(gross 낮음)을 키우고 덜한 달을 줄이므로 수익 경로 자체가
    달라진다 -> Sharpe/MDD 가 미스케일 대비 이동할 수 있다 (단순 리스케일 아님).
    """
    seq = _monthly([0.80, 0.85, 0.75, 0.90], ret=0.02)
    un = build_stock_series(seq, 10.0, None)["net_return"]
    sc = build_stock_series(seq, 10.0, 0.40)["net_return"]
    ratios = (sc / un).round(10)
    assert ratios.nunique() > 1, "배수가 월마다 달라야 경로가 바뀐다"
