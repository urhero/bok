# -*- coding: utf-8 -*-
"""Rich 콘솔 출력의 cp949 안전성 테스트 (CLAUDE.md: em-dash/화살표/이모지 금지)."""
import io

import pytest
from rich.console import Console

import service.report.reporting as reporting
from service.backtest.overfit_diagnostics import generate_overfit_report
from service.backtest.result_stitcher import WalkForwardResult


def test_print_overfit_report_output_is_cp949_safe(monkeypatch):
    """print_overfit_report 출력이 cp949 콘솔에서 인코딩 가능해야 한다.

    Windows cp949 콘솔에서 인코딩 불가 문자(이모지/em-dash)가 있으면
    UnicodeEncodeError 로 `backtest` CLI 가 크래시한다. 실제 생산자
    (generate_overfit_report)가 만든 리포트 dict 를 그대로 출력해 키 계약도 함께 고정한다.
    """
    rec = Console(file=io.StringIO(), record=True, width=200)
    monkeypatch.setattr("rich.console.Console", lambda *a, **k: rec)

    report = generate_overfit_report(WalkForwardResult([]), full_period_cagr=0.0)
    reporting.print_overfit_report(report)

    text = rec.export_text()
    assert "Funnel" in text
    try:
        text.encode("cp949")
    except UnicodeEncodeError as exc:
        pytest.fail(f"overfit report not cp949-safe: {exc!r} in {text!r}")
