# -*- coding: utf-8 -*-
"""Rich 콘솔 출력의 cp949 안전성 테스트 (CLAUDE.md: em-dash/화살표/이모지 금지)."""
import io

import pytest
from rich.console import Console

import service.report.reporting as reporting


@pytest.mark.parametrize("excess_cagr", [-0.01, 0.01])
def test_print_benchmark_report_output_is_cp949_safe(excess_cagr, monkeypatch):
    """print_benchmark_report 출력이 cp949 콘솔에서 인코딩 가능해야 한다.

    Windows cp949 콘솔에서 인코딩 불가 문자(이모지/em-dash)가 있으면
    UnicodeEncodeError로 `mp --benchmark` CLI가 크래시한다.
    excess_cagr 부호로 경고/성공 두 분기를 모두 검증한다.
    """
    rec = Console(file=io.StringIO(), record=True, width=200)
    # print_benchmark_report 내부 `from rich.console import Console` 가 rec을 쓰게 패치
    monkeypatch.setattr("rich.console.Console", lambda *a, **k: rec)

    report = {
        "mp_cagr": 0.10, "ew_cagr": 0.08, "excess_cagr": excess_cagr,
        "mp_mdd": -0.20, "ew_mdd": -0.25, "mp_sharpe": 1.0, "ew_sharpe": 0.9,
        "win_rate": 0.55, "t_statistic": 2.1, "p_value": 0.03,
    }
    reporting.print_benchmark_report(report)

    text = rec.export_text()
    try:
        text.encode("cp949")
    except UnicodeEncodeError as exc:
        pytest.fail(f"benchmark report not cp949-safe: {exc!r} in {text!r}")
