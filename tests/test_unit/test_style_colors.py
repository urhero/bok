# -*- coding: utf-8 -*-
"""STYLE_COLORS 단일 출처(single source of truth) 테스트."""
import pytest


def test_style_colors_leaf_is_single_source():
    """단일 출처 service.report.style_colors 가 8개 스타일(Volatility 포함) + 기본색을 갖는다.

    matplotlib/plotly 무의존이라 헤드리스 CI 에서도 항상 실행된다.
    """
    from service.report.style_colors import STYLE_COLORS, _DEFAULT_COLOR

    assert "Volatility" in STYLE_COLORS, "Volatility 스타일이 누락되면 안 됨"
    # 2026-08-25 다크테마 팔레트 통일 (대시보드 3번 섹션 기준색)
    assert STYLE_COLORS["Valuation"] == "#5B8DEF"
    assert STYLE_COLORS["Historical Growth"] == "#E8944A"
    assert len(STYLE_COLORS) == 8
    assert _DEFAULT_COLOR == "#7f7f7f"


def test_report_and_dashboard_reexport_single_source():
    """report_generator(matplotlib)·dashboard_charts(plotly) 가 동일 STYLE_COLORS 객체를 re-export.

    과거 두 모듈이 각각 정의해 발산했었다(report_generator에 Volatility 누락).
    viz 의존(matplotlib/plotly) 모듈이라 미설치 환경(CI)에서는 skip 한다.
    """
    pytest.importorskip("matplotlib")
    pytest.importorskip("plotly")
    from service.report.style_colors import STYLE_COLORS as src
    from service.report.report_generator import STYLE_COLORS as rg
    from service.report.dashboard_charts import STYLE_COLORS as dc

    assert rg is src, "report_generator 가 단일 출처를 re-export 해야 함"
    assert dc is src, "dashboard_charts 가 단일 출처를 re-export 해야 함"
