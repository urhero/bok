# -*- coding: utf-8 -*-
"""스타일 -> 색상 매핑 단일 출처(single source of truth).

report_generator(PDF)와 dashboard_charts(plotly HTML)가 동일 매핑을 공유한다.
과거 두 모듈이 각각 정의해 발산했었다 (report_generator에 Volatility 누락).
"""

# 2026-08-25: 대시보드 3번 섹션(ERC 무리 표)의 다크테마 팔레트로 통일
# (Valuation 파랑, Historical Growth 주황 등 — 사용자 지정 기준색)
STYLE_COLORS = {
    "Valuation": "#5B8DEF",             # Blue
    "Price Momentum": "#98C379",        # Light Green
    "Earnings Quality": "#E06C75",      # Red
    "Size": "#C77DDA",                  # Purple
    "Analyst Expectations": "#4FBF87",  # Green
    "Historical Growth": "#E8944A",     # Orange
    "Capital Efficiency": "#E5C453",    # Gold (탠은 주황과 혼동돼 교체, 2026-08-25)
    "Volatility": "#56B6C2",            # Cyan
}

_DEFAULT_COLOR = "#7f7f7f"  # 미정의 스타일 fallback (gray)
