# -*- coding: utf-8 -*-
"""STYLE_COLORS 단일 출처(single source of truth) 테스트."""


def test_style_colors_single_source_of_truth():
    """report_generator 와 dashboard_charts 가 동일한 STYLE_COLORS 매핑을 공유해야 한다.

    과거 두 모듈이 STYLE_COLORS를 각각 정의해 발산했다
    (report_generator에 Volatility 누락). 단일 출처로 통합되어야 한다.
    """
    from service.report.report_generator import STYLE_COLORS as rg_colors
    from service.report.dashboard_charts import STYLE_COLORS as dc_colors

    assert rg_colors == dc_colors, "두 모듈의 STYLE_COLORS가 일치해야 함"
    assert "Volatility" in rg_colors, "Volatility 스타일이 누락되면 안 됨"
    assert rg_colors["Volatility"] == "#9467bd"
