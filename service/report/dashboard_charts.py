# -*- coding: utf-8 -*-
"""대시보드 차트 레이어. DataFrame -> plotly Figure.

색상 매핑은 service/report/report_generator.py 의 STYLE_COLORS 를 그대로 따르되,
거기 빠진 Volatility 와 미정의 스타일 fallback 을 보강한다(파이프라인 미수정 위해 복제).
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

from service.report.dashboard_data import compute_drawdown

# report_generator.STYLE_COLORS 미러 + Volatility 보강
STYLE_COLORS = {
    "Valuation": "#d62728",
    "Price Momentum": "#ff7f0e",
    "Earnings Quality": "#e377c2",
    "Size": "#2ca02c",
    "Analyst Expectations": "#17becf",
    "Historical Growth": "#8c564b",
    "Capital Efficiency": "#bcbd22",
    "Volatility": "#9467bd",
}
_DEFAULT_COLOR = "#7f7f7f"

# 전략별 색/라벨 (누적수익 비교)
_STRAT = [
    ("ew_all", "#888780", "전체 EW", 1.6),
    ("ew_top50", "#BA7517", "Top50 EW", 1.8),
    ("ew", "#1D9E75", "선정 EW", 1.8),
    ("cew", "#185FA5", "CEW (본전략)", 2.6),
]

_LONG_COLOR = "#1D9E75"
_SHORT_COLOR = "#D85A30"
_POS_COLOR = "#1D9E75"
_NEG_COLOR = "#E24B4A"

_BASE_LAYOUT = dict(template="plotly_white", margin=dict(l=60, r=20, t=50, b=45))


def _style_color(style: str) -> str:
    return STYLE_COLORS.get(style, _DEFAULT_COLOR)


# ── 백테스트 ───────────────────────────────────────────────────────────────

def equity_curve_fig(curves: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for key, color, label, width in _STRAT:
        col = f"{key}_cumulative"
        if col not in curves.columns:
            continue
        fig.add_trace(go.Scatter(
            x=curves.index, y=curves[col], name=label,
            line=dict(color=color, width=width),
            hovertemplate="%{x|%Y-%m}<br>" + label + " %{y:.4f}<extra></extra>",
        ))
    fig.update_layout(
        title="누적 수익 곡선 (4개 전략 비교)", height=400,
        legend=dict(orientation="h", yanchor="bottom", y=-0.22, x=0),
        yaxis_title="누적 (시작=1)", **_BASE_LAYOUT,
    )
    return fig


def drawdown_fig(curves: pd.DataFrame) -> go.Figure:
    dd = compute_drawdown(curves["cew_cumulative"])
    fig = go.Figure(go.Scatter(
        x=dd.index, y=dd, fill="tozeroy", name="CEW 낙폭",
        line=dict(color=_NEG_COLOR, width=1.4),
        hovertemplate="%{x|%Y-%m}<br>%{y:.2%}<extra></extra>",
    ))
    fig.update_layout(title="낙폭 (drawdown, CEW)", height=320,
                      yaxis_tickformat=".0%", **_BASE_LAYOUT)
    return fig


def monthly_dist_fig(curves: pd.DataFrame) -> go.Figure:
    r = curves["cew_return"].astype(float)
    fig = go.Figure(go.Histogram(x=r, nbinsx=40, marker_color="#378ADD"))
    fig.add_vline(x=0, line_dash="dash", line_color="#888780")
    fig.update_layout(title="월별 수익 분포 (CEW)", height=320,
                      xaxis_tickformat=".1%", bargap=0.03,
                      yaxis_title="개월 수", **_BASE_LAYOUT)
    return fig


# ── 현재 포트 / 배팅 ───────────────────────────────────────────────────────

def style_allocation_fig(style_weights: pd.Series, style_cap: float = 0.25) -> go.Figure:
    s = style_weights.sort_values(ascending=True)
    fig = go.Figure(go.Bar(
        x=s.values, y=s.index, orientation="h",
        marker_color=[_style_color(st) for st in s.index],
        hovertemplate="%{y}<br>%{x:.2%}<extra></extra>",
    ))
    fig.add_vline(x=style_cap, line_dash="dash", line_color=_NEG_COLOR,
                  annotation_text=f"cap {style_cap:.0%}", annotation_position="top")
    fig.update_layout(title="스타일 배분 (style_cap 라인)", height=360,
                      xaxis_tickformat=".0%", **_BASE_LAYOUT)
    return fig


def longs_shorts_fig(ls_df: pd.DataFrame) -> go.Figure:
    d = ls_df.sort_values("weight")
    colors = [_LONG_COLOR if s == "long" else _SHORT_COLOR for s in d["side"]]
    fig = go.Figure(go.Bar(
        x=d["weight"], y=d["ticker"], orientation="h", marker_color=colors,
        hovertemplate="%{y}<br>%{x:.3%}<extra></extra>",
    ))
    fig.update_layout(title="종목별 순비중 상위 롱/숏", height=max(360, 22 * len(d) + 80),
                      xaxis_tickformat=".2%", **_BASE_LAYOUT)
    return fig


def factor_tilt_fig(tilt_df: pd.DataFrame, top_n: int = 25) -> go.Figure:
    d = tilt_df.head(top_n).sort_values("factor_weight")
    fig = go.Figure(go.Bar(
        x=d["factor_weight"], y=d["factor"], orientation="h",
        marker_color=[_style_color(st) for st in d["style"]],
        customdata=d["style"],
        hovertemplate="%{y}<br>%{x:.3%}<br>%{customdata}<extra></extra>",
    ))
    n_shown = min(top_n, len(tilt_df))
    fig.update_layout(title=f"팩터 틸트 (상위 {n_shown}, 스타일별 색)",
                      height=max(360, 20 * len(d) + 80), xaxis_tickformat=".2%", **_BASE_LAYOUT)
    return fig


def leaderboard_fig(meta: pd.DataFrame, selected: set) -> go.Figure:
    m = meta.copy()
    m["selected"] = m["factorAbbreviation"].isin(selected)
    fig = go.Figure()
    for flag, color, label, size in [
        (False, "#B4B2A9", "미선정", 6),
        (True, "#185FA5", "선정", 9),
    ]:
        sub = m[m["selected"] == flag]
        fig.add_trace(go.Scatter(
            x=sub["tstat"], y=sub["cagr"], mode="markers", name=label,
            marker=dict(color=color, size=size, line=dict(width=0)),
            text=sub["factorAbbreviation"], customdata=sub["styleName"],
            hovertemplate="%{text} (%{customdata})<br>tstat %{x:.2f}<br>CAGR %{y:.2%}<extra></extra>",
        ))
    fig.update_layout(title="팩터 리더보드 (tstat vs CAGR)", height=420,
                      xaxis_title="tstat", yaxis_title="CAGR", yaxis_tickformat=".0%",
                      legend=dict(orientation="h", yanchor="bottom", y=-0.25, x=0),
                      **_BASE_LAYOUT)
    return fig


def sector_net_fig(sector_series: pd.Series) -> go.Figure:
    s = sector_series.sort_values()
    colors = [_POS_COLOR if v >= 0 else _NEG_COLOR for v in s.values]
    fig = go.Figure(go.Bar(
        x=s.values, y=s.index, orientation="h", marker_color=colors,
        hovertemplate="%{y}<br>%{x:.3%}<extra></extra>",
    ))
    fig.add_vline(x=0, line_color="#888780", line_width=1)
    fig.update_layout(title="섹터별 순비중 (롱-숏 순노출)", height=360,
                      xaxis_tickformat=".2%", **_BASE_LAYOUT)
    return fig


def turnover_fig(turnover: pd.Series) -> go.Figure:
    fig = go.Figure(go.Bar(
        x=turnover.index, y=turnover.values, marker_color="#534AB7",
        hovertemplate="%{x|%Y-%m}<br>%{y:.2%}<extra></extra>",
    ))
    avg = float(turnover[turnover > 0].mean()) if (turnover > 0).any() else 0.0
    fig.add_hline(y=avg, line_dash="dash", line_color="#888780",
                  annotation_text=f"평균 {avg:.1%}", annotation_position="top left")
    fig.update_layout(title="팩터 회전율 (one-way, 리밸런싱 시점)", height=320,
                      yaxis_tickformat=".0%", **_BASE_LAYOUT)
    return fig


def style_weight_evolution_fig(style_hist: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    # 평균 비중 큰 스타일이 아래로 깔리도록 정렬
    order = style_hist.mean().sort_values(ascending=False).index
    for st in order:
        fig.add_trace(go.Scatter(
            x=style_hist.index, y=style_hist[st], name=st, mode="lines",
            stackgroup="one", line=dict(width=0.5, color=_style_color(st)),
            hovertemplate="%{x|%Y-%m}<br>" + str(st) + " %{y:.1%}<extra></extra>",
        ))
    fig.update_layout(title="스타일 비중 추이 (백테스트)", height=380,
                      yaxis_tickformat=".0%",
                      legend=dict(orientation="h", yanchor="bottom", y=-0.25, x=0),
                      **_BASE_LAYOUT)
    return fig


def style_delta_fig(deltas: pd.DataFrame) -> go.Figure:
    d = deltas.sort_values("delta")
    colors = [_POS_COLOR if v >= 0 else _NEG_COLOR for v in d["delta"]]
    fig = go.Figure(go.Bar(
        x=d["style"], y=d["delta"], marker_color=colors,
        hovertemplate="%{x}<br>%{y:+.2%}<extra></extra>",
    ))
    fig.update_layout(title="전월 대비 스타일 비중 변화", height=340,
                      yaxis_tickformat=".1%", xaxis_tickangle=-30, **_BASE_LAYOUT)
    return fig
