# -*- coding: utf-8 -*-
"""스타일 -> 색상 매핑 단일 출처(single source of truth).

report_generator(PDF)와 dashboard_charts(plotly HTML)가 동일 매핑을 공유한다.
과거 두 모듈이 각각 정의해 발산했었다 (report_generator에 Volatility 누락).
"""

STYLE_COLORS = {
    "Valuation": "#d62728",             # Red
    "Price Momentum": "#ff7f0e",        # Orange
    "Earnings Quality": "#e377c2",      # Bright Pink
    "Size": "#2ca02c",                  # Green
    "Analyst Expectations": "#17becf",  # Cyan / Teal
    "Historical Growth": "#8c564b",     # Brown
    "Capital Efficiency": "#bcbd22",    # Olive (high-contrast yellow-green)
    "Volatility": "#9467bd",            # Purple
}

_DEFAULT_COLOR = "#7f7f7f"  # 미정의 스타일 fallback (gray)
