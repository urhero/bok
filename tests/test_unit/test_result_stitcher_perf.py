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


def test_contribution_history_built_and_sums_to_oos_return():
    """contributions 키가 있으면 결정적(알파벳 컬럼) 이력이 만들어지고 행합=oos_return."""
    records = [
        {"date": pd.Timestamp("2024-01-31"), "oos_return": 0.012, "oos_ew_return": 0.01,
         "contributions": {"B": 0.004, "A": 0.008}},
        {"date": pd.Timestamp("2024-02-29"), "oos_return": -0.002, "oos_ew_return": 0.0,
         "contributions": {"A": -0.002}},
    ]
    res = WalkForwardResult(records)
    ch = res.contribution_history
    assert list(ch.columns) == ["A", "B"]
    sums = ch.sum(axis=1).tolist()
    assert math.isclose(sums[0], 0.012) and math.isclose(sums[1], -0.002)


def test_contribution_history_empty_without_key():
    """구버전 레코드(contributions 없음)와 빈 결과 모두 빈 DataFrame (하위 호환)."""
    records = [{"date": pd.Timestamp("2024-01-31"), "oos_return": 0.0, "oos_ew_return": 0.0}]
    assert WalkForwardResult(records).contribution_history.empty
    assert WalkForwardResult([]).contribution_history.empty
