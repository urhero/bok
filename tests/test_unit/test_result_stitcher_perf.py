# -*- coding: utf-8 -*-
"""WalkForwardResult._calc_perf 성과 계산 엣지케이스 테스트."""
import math

import pandas as pd

from service.backtest.result_stitcher import WalkForwardResult


def test_calc_perf_total_loss_returns_real_cagr_not_nan():
    """터미널 누적값이 비양수(전손)면 CAGR은 nan이 아닌 실수(-1.0)여야 한다.

    nan이면 Funnel Value-Add의 `>` 비교가 항상 False가 되어
    재앙적 드로다운 구간을 조용히 NORMAL로 오분류한다.
    """
    # 둘째 달 -150% -> (1+r) 음수 -> 누적이 음수로 전환, 비정수 연율화 지수
    returns = pd.Series([0.0, -1.5, 0.0, 0.0, 0.0])
    cumulative = (1.0 + returns).cumprod()  # [1, -0.5, -0.5, -0.5, -0.5]
    assert cumulative.iloc[-1] <= 0  # 전제 확인 (전손)

    perf = WalkForwardResult._calc_perf(returns, cumulative)

    assert not math.isnan(perf["cagr"]), "전손 시 CAGR이 nan이면 안 됨"
    assert perf["cagr"] == -1.0
    assert not math.isnan(perf["calmar"]), "CAGR nan이 calmar로 전파되면 안 됨"


def test_calc_perf_normal_path_unchanged():
    """정상(양수 누적) 경로는 기존 공식과 byte-identical해야 한다 (회귀 가드)."""
    returns = pd.Series([0.0, 0.02, 0.01, -0.005, 0.03])
    cumulative = (1.0 + returns).cumprod()
    months = len(returns)
    expected_cagr = cumulative.iloc[-1] ** (12 / months) - 1

    perf = WalkForwardResult._calc_perf(returns, cumulative)

    assert perf["cagr"] == expected_cagr
