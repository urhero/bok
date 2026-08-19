# -*- coding: utf-8 -*-
"""별첨 리포트 북 생성 (세로 A4 재설계, 2026-08-06).

Claude Design 핸드오프(세로 A4 북 재설계안)를 matplotlib 로 구현한다.
디자인 원본: HTML/SVG 프로토타입 — 각 차트는 프로토타입의 SVG viewBox 좌표계를
matplotlib axes(xlim/ylim=px)에 1:1 이식해 픽셀 정합을 유지한다.

  별첨02: Sector x Quintile Return Book  (섹터별 5분위 막대, 비투자 섹터 회색)
  별첨03: Quintile Return Book           (5분위 막대, L/S/N 실제 라벨 색)
  별첨04: Long-Short Portfolio Return Book (L-S 누적수익 시계열)
  별첨01: Factor_Return_Info.xlsx        (팩터 L-S 수익률 + 카테고리)

공통: 커버 페이지 + 페이지당 6팩터 카드(별첨03은 2열 그리드), L-S CAGR 내림차순.
"""
from __future__ import annotations

import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.lines import Line2D
from matplotlib.patches import Ellipse, Rectangle

from config import PARAM, PIPELINE_PARAMS
from service.factor.factor_returns import aggregate_factor_returns
from service.paths import HISTORY_DIR, OUTPUT_DIR, latest
from service.pipeline.factor_analysis import filter_and_label_factors
from service.report.style_colors import STYLE_COLORS, _DEFAULT_COLOR

logger = logging.getLogger(__name__)

_BM = PARAM["benchmark"]

RENAME_SECTORS = {
    "Communication Services": "CS",
    "Consumer Discretionary": "Cons. Disc.",
    "Consumer Staples": "Cons. Stap.",
    "Information Technology": "IT",
}

# ── 디자인 토큰 (핸드오프 프로토타입 그대로) ─────────────────────────────────
PAGE_W, PAGE_H = 794.0, 1123.0          # A4 @96dpi css px
INK = "#1c1e21"        # 본문/룰
SUB = "#5a5d60"        # 보조 텍스트
MUTE = "#8a8d90"       # 3차 텍스트
FAINT = "#b0b2b4"      # 4차 텍스트 (rank, footer)
GRID = "#e8e8e5"
ZERO = "#b6b8ba"
RAMP = ["#0f4c5c", "#2e6f80", "#5b93a3", "#93b9c4", "#cfe0e6"]  # Q1->Q5
DROP = "#e5e6e4"       # 비투자 섹터 클러스터
DROP_LABEL = "#c2c4c2"
LONG_C, SHORT_C, NEUT_C = "#0f4c5c", "#b23a48", "#e0e2e2"
PER_PAGE = 6
_FONT = ["Arial", "Helvetica", "DejaVu Sans"]


def _pt(px: float) -> float:
    return px * 0.75


def _text(fig, xpx, ypx, s, px, color=INK, weight="normal", ha="left", va="top"):
    fig.text(xpx / PAGE_W, 1.0 - ypx / PAGE_H, s, fontsize=_pt(px), color=color,
             fontweight=weight, ha=ha, va=va, family=_FONT)


def _hline(fig, x0px, x1px, ypx, color=INK, lw=0.8):
    fig.add_artist(Line2D([x0px / PAGE_W, x1px / PAGE_W],
                          [1.0 - ypx / PAGE_H] * 2, color=color, lw=lw,
                          transform=fig.transFigure))


def _swatch(fig, xpx, ypx, wpx, hpx, color, circle=False):
    if circle:
        # figure 좌표는 비정방(794x1123)이라 Circle 은 타원으로 왜곡 — Ellipse 로 보정
        fig.add_artist(Ellipse((xpx / PAGE_W + wpx / 2 / PAGE_W,
                                1.0 - (ypx + hpx / 2) / PAGE_H),
                               wpx / PAGE_W, hpx / PAGE_H, facecolor=color,
                               edgecolor="none", transform=fig.transFigure))
    else:
        fig.add_artist(Rectangle((xpx / PAGE_W, 1.0 - (ypx + hpx) / PAGE_H),
                                 wpx / PAGE_W, hpx / PAGE_H, facecolor=color,
                                 edgecolor="none", transform=fig.transFigure))


def _svg_axes(fig, xpx, ypx, wpx, hpx, vw, vh):
    """프로토타입 SVG viewBox(0 0 vw vh) 좌표계 axes (y 아래방향)."""
    ax = fig.add_axes([xpx / PAGE_W, 1.0 - (ypx + hpx) / PAGE_H,
                       wpx / PAGE_W, hpx / PAGE_H])
    ax.set_xlim(0, vw)
    ax.set_ylim(vh, 0)
    ax.axis("off")
    return ax


def _nice_step(span, candidates, max_ticks):
    for s in candidates:
        if span / s <= max_ticks:
            return s
    return candidates[-1]


def _fmt_tick(t):
    return f"{t:g}" if float(t) == int(t) else f"{t:.2f}".rstrip("0")


# ── 커버 페이지 ──────────────────────────────────────────────────────────────
def _cover_page(pp, appendix_no, title_lines, desc, style_counts, cover_date,
                howto_draw, n_inport=0):
    fig = plt.figure(figsize=(8.27, 11.69))
    L, R, T = 64.0, PAGE_W - 64.0, 72.0
    _text(fig, L, T, f"APPENDIX {appendix_no} · {_BM} UNIVERSE", 12, MUTE)
    y = T + 26
    for line in title_lines:
        _text(fig, L, y, line, 40, INK, weight="bold")
        y += 50
    y += 4
    for line in desc:
        _text(fig, L, y, line, 14, SUB)
        y += 22
    y += 24
    _hline(fig, L, R, y, INK, lw=0.9)
    y += 28
    col2 = L + (R - L) / 2 + 20
    _text(fig, L, y, "HOW TO READ", 11, MUTE)
    _text(fig, col2, y, "FACTOR STYLES", 11, MUTE)
    y_body = y + 24
    howto_draw(fig, L, y_body)                       # 북별 커스텀 블록
    # 스타일 범례 2열 (dot / name / count)
    items = [(s, STYLE_COLORS.get(s, _DEFAULT_COLOR), n)
             for s, n in style_counts.items()]
    col_w = (R - col2) / 2 + 4
    for i, (name, c, n) in enumerate(items):
        cx = col2 + (i % 2) * col_w
        cy = y_body + (i // 2) * 22
        _swatch(fig, cx, cy + 2, 9, 9, c, circle=True)
        _text(fig, cx + 17, cy, name, 11.5, INK)
        _text(fig, cx + col_w - 6, cy, str(n), 12, MUTE, ha="right")
    # 실사용(편입) 팩터 표기 범례
    if n_inport:
        cy = y_body + ((len(items) + 1) // 2) * 22 + 12
        _rank_badge(fig, col2 + 4, cy, 1, True)
        _text(fig, col2 + 34, cy, f"In current model portfolio ({n_inport} factors)",
              11.5, SUB)
    _text(fig, L, PAGE_H - 56, _BM, 11, MUTE, va="bottom")
    _text(fig, R, PAGE_H - 56, cover_date, 11, MUTE, ha="right", va="bottom")
    pp.savefig(fig)
    plt.close(fig)


# ── 콘텐츠 페이지 골격 ───────────────────────────────────────────────────────
def _page_frame(fig, header_title, page_no, page_total, footer_l, footer_r,
                header_legend_draw=None):
    L, R = 56.0, PAGE_W - 56.0
    _text(fig, L, 40, header_title.upper(), 10, SUB)
    _text(fig, R, 40, f"{page_no} / {page_total}", 10, MUTE, ha="right")
    if header_legend_draw:
        header_legend_draw(fig, R)
    _hline(fig, L, R, 60, INK, lw=0.8)
    _text(fig, L, PAGE_H - 30, footer_l, 9, FAINT, va="bottom")
    _text(fig, R, PAGE_H - 30, footer_r, 9, FAINT, ha="right", va="bottom")


def _rank_badge(fig, xpx, ypx, rank, in_port):
    """랭크 번호. 실사용(포트폴리오 편입) 팩터는 잉크색 배지로 반전 표시."""
    if in_port:
        _swatch(fig, xpx - 4, ypx - 1, 27, 13, INK)
        _text(fig, xpx + 9.5, ypx + 0.5, f"{rank:03d}", 9, "#ffffff",
              weight="bold", ha="center")
    else:
        _text(fig, xpx, ypx, f"{rank:03d}", 9, FAINT)


def _card_header(fig, xpx, ypx, rank, color, name, style=None, cagr_label=None,
                 wpx=682.0, in_port=False):
    _rank_badge(fig, xpx, ypx, rank, in_port)
    _swatch(fig, xpx + 27, ypx + 1, 8, 8, color, circle=True)
    _text(fig, xpx + 41, ypx - 1.5, name, 11.5, INK, weight="bold")
    if cagr_label is not None:
        _text(fig, xpx + wpx, ypx, cagr_label, 10, SUB, ha="right")
    if style is not None:
        off = 96 if cagr_label else 0
        _text(fig, xpx + wpx - off, ypx, style, 10, MUTE, ha="right")


def _pct(v):
    return f"{'+' if v >= 0 else ''}{v:.1f}%"


# ── 차트 3종 (프로토타입 SVG 좌표 이식) ──────────────────────────────────────
def _chart_sector(ax, sec_vals, sectors, dropped_idx, svg_h):
    """별첨02: 섹터 클러스터 x Q1~Q5 막대. sec_vals: (n_sector, 5) %."""
    plot_l, plot_r, plot_t, plot_b = 30.0, 682.0, 8.0, svg_h - 26.0
    flat = sec_vals.flatten()
    vmin, vmax = min(0.0, flat.min()), max(0.001, flat.max())
    span = vmax - vmin
    step = _nice_step(span, [0.25, 0.5, 1, 2, 5, 10], 4.5)

    def y(v):
        return plot_b - (v - vmin) / span * (plot_b - plot_t)

    t = np.ceil(vmin / step) * step
    while t <= vmax + 1e-9:
        if abs(t) > 1e-9:
            ax.plot([plot_l, plot_r], [y(t)] * 2, color=GRID, lw=0.8)
            ax.text(plot_l - 4, y(t), _fmt_tick(t), fontsize=_pt(8.5),
                    color=MUTE, ha="right", va="center", family=_FONT)
        t += step
    ax.plot([plot_l, plot_r], [y(0)] * 2, color=ZERO, lw=0.8)

    gw = (plot_r - plot_l) / len(sectors)
    for s_i, sec in enumerate(sectors):
        dropped = s_i in dropped_idx
        cx = plot_l + s_i * gw + gw / 2
        for q in range(5):
            v = sec_vals[s_i, q]
            y1, y2 = y(max(0, v)), y(min(0, v))
            ax.add_patch(plt.Rectangle(
                (cx - 19.5 + q * 8, y1), 7, max(0.5, y2 - y1),
                facecolor=(DROP if dropped else RAMP[q]), edgecolor="none", zorder=3))
        ax.text(cx, svg_h - 6, sec, fontsize=_pt(8.5),
                color=(DROP_LABEL if dropped else SUB), ha="center",
                va="bottom", family=_FONT)


def _chart_quintile(ax, vals, labels, svg_h, svg_w=320.0):
    """별첨03: 5분위 막대 (L/S/N 실제 라벨 색) + 값/분위 라벨."""
    plot_l, plot_r, plot_t, plot_b = 26.0, svg_w - 8.0, 12.0, svg_h - 30.0
    vmin, vmax = min(0.0, min(vals)), max(0.001, max(vals))
    span = vmax - vmin
    step = _nice_step(span, [0.25, 0.5, 1, 2, 5, 10], 4)

    def y(v):
        return plot_b - (v - vmin) / span * (plot_b - plot_t)

    t = np.ceil(vmin / step) * step
    while t <= vmax + 1e-9:
        if abs(t) > 1e-9:
            ax.plot([plot_l, plot_r], [y(t)] * 2, color=GRID, lw=0.8)
            ax.text(plot_l - 4, y(t), _fmt_tick(t), fontsize=_pt(9),
                    color=MUTE, ha="right", va="center", family=_FONT)
        t += step
    ax.plot([plot_l, plot_r], [y(0)] * 2, color=ZERO, lw=0.8)

    slot = (plot_r - plot_l) / 5
    for q, v in enumerate(vals):
        cx = plot_l + q * slot + slot / 2
        lab = labels[q]
        col = LONG_C if lab == "L" else SHORT_C if lab == "S" else NEUT_C
        y1, y2 = y(max(0, v)), y(min(0, v))
        ax.add_patch(plt.Rectangle((cx - 17, y1), 34, max(0.5, y2 - y1),
                                   facecolor=col, edgecolor="none", zorder=3))
        vy = y1 - 4 if v >= 0 else y2 + 10
        ax.text(cx, vy, f"{v:.2f}", fontsize=_pt(9),
                color=("#a5a8a9" if lab == "N" else SUB), ha="center",
                va="bottom" if v >= 0 else "top", family=_FONT)
        ax.text(cx, svg_h - 8, f"Q{q + 1}", fontsize=_pt(9.5),
                fontweight=("normal" if lab == "N" else "bold"),
                color=(LONG_C if lab == "L" else SHORT_C if lab == "S" else MUTE),
                ha="center", va="bottom", family=_FONT)


def _chart_ls_series(ax, series, color, svg_h):
    """별첨04: L-S 누적수익(%) 라인 + 면 + 연도 눈금 + 기말값."""
    plot_l, plot_r, plot_t, plot_b = 34.0, 640.0, 8.0, svg_h - 20.0
    sv = series.values
    n = len(sv)
    vmin, vmax = min(0.0, sv.min()), max(0.001, sv.max())
    span = vmax - vmin
    step = _nice_step(span, [5, 10, 20, 25, 50, 100], 4.5)

    def y(v):
        return plot_b - (v - vmin) / span * (plot_b - plot_t)

    def x(i):
        return plot_l + i / (n - 1) * (plot_r - plot_l)

    t = np.ceil(vmin / step) * step
    while t <= vmax + 1e-9:
        if abs(t) > 1e-9:
            ax.plot([plot_l, plot_r], [y(t)] * 2, color=GRID, lw=0.8)
            ax.text(plot_l - 4, y(t), f"{t:g}", fontsize=_pt(8.5), color=MUTE,
                    ha="right", va="center", family=_FONT)
        t += step
    ax.plot([plot_l, plot_r], [y(0)] * 2, color=ZERO, lw=0.8)

    for i, d in enumerate(series.index):
        if d.month == 1 and i > 0:               # 매년 1월 세로 눈금
            ax.plot([x(i)] * 2, [plot_t, plot_b], color="#f0f0ee", lw=0.8)
            ax.text(x(i), plot_b + 14, str(d.year), fontsize=_pt(8.5),
                    color=MUTE, ha="center", va="center", family=_FONT)

    xs = [x(i) for i in range(n)]
    ys = [y(v) for v in sv]
    ax.fill_between(xs, ys, y(0), color=color, alpha=0.08, linewidth=0, zorder=2.5)
    ax.plot(xs, ys, color=color, lw=1.2, zorder=3)
    ax.text(plot_r + 5, ys[-1], _pct(sv[-1]), fontsize=_pt(9),
            fontweight="bold", color=color, ha="left", va="center",
            family=_FONT, clip_on=False)


# ── 북 렌더러 ────────────────────────────────────────────────────────────────
def _locked(pdf_path):
    """출력 파일이 다른 프로그램(뷰어/Excel)에 열려 있으면 True."""
    try:
        with open(pdf_path, "ab"):
            return False
    except PermissionError:
        logger.warning("%s 가 잠겨 있어(열려 있음) 이 북은 생략 — 닫고 재실행 필요", pdf_path.name)
        return True
    except FileNotFoundError:
        return False


def _render_stacked_book(pdf_path, records, header_title, footer_l, footer_r,
                         cover_fn, chart_fn, cagr_prefix,
                         header_legend_draw=None):
    """별첨02/04 공통: 1열 x 6카드/페이지."""
    if _locked(pdf_path):
        return
    L = 56.0
    card_w = PAGE_W - 112.0                     # 682
    svg_h = max(56, min(150, int(983 / PER_PAGE) - 34))
    n_pages = (len(records) + PER_PAGE - 1) // PER_PAGE
    with PdfPages(pdf_path) as pp:
        cover_fn(pp)
        top, bottom = 74.0, PAGE_H - 56.0
        slot_h = (bottom - top) / PER_PAGE
        for p in range(n_pages):
            fig = plt.figure(figsize=(8.27, 11.69))
            _page_frame(fig, header_title, p + 1, n_pages, footer_l, footer_r,
                        header_legend_draw)
            for ci, rec in enumerate(records[p * PER_PAGE:(p + 1) * PER_PAGE]):
                y0 = top + ci * slot_h
                _card_header(fig, L, y0, rec["rank"], rec["color"], rec["name"],
                             rec["style"], f"{cagr_prefix} {_pct(rec['cagr'])}",
                             wpx=card_w, in_port=rec["in_port"])
                ax = _svg_axes(fig, L, y0 + 18, card_w, svg_h, 682.0, svg_h)
                chart_fn(ax, rec, svg_h)
            pp.savefig(fig)
            plt.close(fig)
    logger.info("Saved %s (%d pages + cover)", pdf_path.name, n_pages)


def _render_grid_book03(pdf_path, records, cover_fn):
    """별첨03: 2열 x 3행 그리드."""
    if _locked(pdf_path):
        return
    L = 56.0
    rows = PER_PAGE // 2
    svg_h = max(96, min(148, int(963 / rows) - 56))
    col_w = (PAGE_W - 112.0 - 28.0) / 2          # 327
    n_pages = (len(records) + PER_PAGE - 1) // PER_PAGE

    def legend(fig, right_px):
        x = right_px - 60
        for lab, col in (("Neutral", NEUT_C), ("Short", SHORT_C), ("Long", LONG_C)):
            _text(fig, x, 41, lab, 9, MUTE, ha="right")
            _swatch(fig, x - _pt(len(lab)) * 5.6 - 13, 41.5, 9, 9, col)
            x -= _pt(len(lab)) * 5.6 + 30

    with PdfPages(pdf_path) as pp:
        cover_fn(pp)
        top, bottom = 76.0, PAGE_H - 56.0
        chart_h = svg_h * col_w / 320.0
        card_h = 30.0 + chart_h
        # 디자인 스크린샷 기준 컴팩트 스택 (행간 20px, 상단 정렬)
        row_gap = 20.0
        for p in range(n_pages):
            fig = plt.figure(figsize=(8.27, 11.69))
            _page_frame(fig, f"{_BM} · Quintile Returns · Long / Short Selection",
                        p + 1, n_pages, "Appendix 03 — Quintile Return Book",
                        "Avg monthly return, %", legend)
            for ci, rec in enumerate(records[p * PER_PAGE:(p + 1) * PER_PAGE]):
                r, c = ci // 2, ci % 2
                x0 = L + c * (col_w + 28.0)
                y0 = top + r * (card_h + row_gap)
                name = rec["name"] if len(rec["name"]) <= 62 else rec["name"][:59] + "..."
                _rank_badge(fig, x0, y0, rec["rank"], rec["in_port"])
                _swatch(fig, x0 + 26, y0 + 1, 8, 8, rec["color"], circle=True)
                _text(fig, x0 + 39, y0 - 1, name, 10.5, INK, weight="bold")
                _text(fig, x0 + 20, y0 + 15, rec["style"], 9, MUTE)
                _text(fig, x0 + col_w, y0 + 15, f"L–S CAGR {_pct(rec['cagr'])}",
                      9, SUB, ha="right")
                ax = _svg_axes(fig, x0, y0 + 30, col_w, chart_h,
                               320.0, svg_h)
                _chart_quintile(ax, rec["quint_vals"], rec["quint_labels"], svg_h)
            pp.savefig(fig)
            plt.close(fig)
    logger.info("Saved %s (%d pages + cover)", pdf_path.name, n_pages)


# ── 메인 엔트리 ──────────────────────────────────────────────────────────────
def generate_report(factor_abbrs, factor_names, style_names, factor_stats,
                    end_date: str | None = None):
    """별첨 01~04 생성. end_date = 기준일(파일명 접미사). None 이면 최신 스냅샷일."""
    logger.info("Starting generate_report (A4 book redesign)...")
    plt.ioff()

    (kept_abbr, _kept_name, _kept_style, kept_idx, dropped_sec, cleaned_raw,
     ) = filter_and_label_factors(factor_abbrs, factor_names, style_names, factor_stats)
    # 별첨은 전체 이력 표시 (backtest_start 기본값 2017-12 절단 우회; 2026-08-06)
    factor_rets = aggregate_factor_returns(
        cleaned_raw, kept_abbr, backtest_start="1900-01-01",
        cost_bps=float(PIPELINE_PARAMS.get("transaction_cost_bps", 20.0)))
    factor_rets.loc[factor_rets.index[0]] = 0.0
    factor_rets = factor_rets.sort_index()
    valid = factor_rets.columns[(factor_rets == 0).sum() <= 10]
    factor_rets = factor_rets[valid]

    meta_df = (pd.read_csv(latest(OUTPUT_DIR / "meta_data.csv"), index_col=0)
               .sort_values(by="cagr", ascending=False).reset_index()
               .rename(columns={"index": "factorAbbreviation"}))

    # ── 실사용(편입) 팩터 집합: 최신 mp 실행이 저장한 factor_weights_*.csv ──
    port_weights: dict[str, float] = {}
    weight_files = sorted(HISTORY_DIR.glob("factor_weights_*.csv"))
    if weight_files and end_date:
        # 기준일 이후 스냅샷은 배제 — 과거 기준일로 북을 다시 뽑을 때 최신 포트가
        # 섞여 들어가는 것을 막는다 (2026-08-19).
        cutoff = f"factor_weights_{str(end_date)[:10]}.csv"
        eligible = [f for f in weight_files if f.name <= cutoff]
        weight_files = eligible or weight_files
    if weight_files:
        wdf = pd.read_csv(weight_files[-1])
        port_weights = {r["factor"]: float(r["weight"]) for _, r in wdf.iterrows()
                        if float(r["weight"]) > 1e-12}
        logger.info("Portfolio marks from %s (%d factors)",
                    weight_files[-1].name, len(port_weights))
    else:
        logger.warning("factor_weights_*.csv not found in %s - 편입 표시 생략", HISTORY_DIR)

    # 파일명 기준일: 인자 > 최신 factor_weights 파일명 > 오늘
    as_of = end_date or (weight_files[-1].stem.replace("factor_weights_", "")
                         if weight_files else pd.Timestamp.now().strftime("%Y-%m-%d"))
    as_of = pd.Timestamp(as_of).strftime("%Y-%m-%d")
    logger.info("별첨 기준일: %s", as_of)

    # ── 별첨01 xlsx ──
    xlsx_path = OUTPUT_DIR / f"별첨01_{_BM}_Factor_Return_Info_{as_of}.xlsx"
    info_df = meta_df[["factorAbbreviation", "factorName", "styleName", "cagr"]].copy()
    info_df["in_portfolio"] = info_df["factorAbbreviation"].map(
        lambda a: "Y" if a in port_weights else "")
    info_df["port_weight"] = info_df["factorAbbreviation"].map(
        lambda a: port_weights.get(a, ""))
    try:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xw:
            info_df.to_excel(xw, sheet_name="Factor_Info", index=False)
            ordered = [c for c in info_df["factorAbbreviation"] if c in factor_rets.columns]
            factor_rets[ordered].to_excel(xw, sheet_name="LS_Monthly_Returns")
        logger.info("Factor return info xlsx saved to %s", xlsx_path)
    except PermissionError:
        # Excel 등에서 파일을 열어둔 경우 — xlsx 만 건너뛰고 PDF 북은 계속 생성
        logger.warning("%s 가 잠겨 있어(열려 있음) xlsx 저장 생략 — 닫고 재실행 필요", xlsx_path.name)

    # ── 팩터 레코드 구성 (CAGR 내림차순, 3개 북 공용) ──
    cum_rets = ((1 + factor_rets).cumprod() - 1) * 100.0
    abbr_to_kept = {a: i for i, a in enumerate(kept_abbr)}
    records = []
    for _, row in meta_df.iterrows():
        abbr = row["factorAbbreviation"]
        ki = abbr_to_kept.get(abbr)
        if ki is None or abbr not in cum_rets.columns:
            continue
        sec_df = factor_stats[kept_idx[ki]][0]
        if sec_df is None:
            continue
        sec_df = sec_df.rename(columns=RENAME_SECTORS) * 100.0
        sectors = list(sec_df.columns)
        dropped_names = {RENAME_SECTORS.get(s, s) for s in dropped_sec[ki]}
        fd = cleaned_raw[ki]
        by_q = fd.groupby("quantile", observed=False)
        q_mean = (by_q["M_RETURN"].mean() * 100.0)
        q_label_raw = by_q["label"].first()
        quint_vals, quint_labels = [], []
        for q in ["Q1", "Q2", "Q3", "Q4", "Q5"]:
            quint_vals.append(float(q_mean.get(q, 0.0)))
            lv = q_label_raw.get(q, 0)
            quint_labels.append("L" if lv == 1 else "S" if lv == -1 else "N")
        style = row["styleName"]
        records.append({
            "rank": len(records) + 1,
            "abbr": abbr,
            "name": row["factorName"],
            "style": style,
            "color": STYLE_COLORS.get(style, _DEFAULT_COLOR),
            "cagr": float(row["cagr"]) * 100.0,
            "sector_vals": sec_df.T.to_numpy(),          # (섹터 x Q1~Q5)
            "sectors": sectors,
            "dropped_idx": {i for i, s in enumerate(sectors) if s in dropped_names},
            "quint_vals": quint_vals,
            "quint_labels": quint_labels,
            "series": cum_rets[abbr],
            "in_port": abbr in port_weights,
        })

    style_counts = {}
    for s in STYLE_COLORS:
        n = sum(1 for r in records if r["style"] == s)
        if n:
            style_counts[s] = n
    n_inport = sum(1 for r in records if r["in_port"])
    # 커버 우하단 날짜 = 생성일이 아니라 데이터 기준일 (파일명과 일치, 2026-08-19)
    cover_date = f"AS OF {pd.Timestamp(as_of).strftime('%d %b %Y').upper()}"
    n_months = len(cum_rets)
    start_label = cum_rets.index[0].strftime("%B %Y")
    cost_bps = int(PIPELINE_PARAMS.get("transaction_cost_bps", 10))

    # ── 별첨02 ──
    def cover02(pp):
        def howto(fig, x, y):
            for i, c in enumerate(RAMP):
                _swatch(fig, x + i * 52, y, 14, 14, c)
                _text(fig, x + i * 52 + 19, y + 1, f"Q{i + 1}", 11, SUB)
            _text(fig, x, y + 30, "Each cluster of five bars is one sector; bars run", 12, SUB)
            _text(fig, x, y + 48, "Q1 (highest factor score) to Q5 (lowest), dark to light.", 12, SUB)
            _swatch(fig, x, y + 74, 10, 10, DROP)
            _text(fig, x + 16, y + 73, "Grey clusters mark sectors excluded from the", 12, SUB)
            _text(fig, x, y + 91, "factor — the Q1–Q5 spread is negative there.", 12, SUB)
        _cover_page(pp, "02", ["Sector × Quintile", "Return Book"],
                    ["Average monthly returns of each factor's quintile",
                     f"portfolios, by GICS sector. {len(records)} factors,",
                     "ordered by in-sample L–S CAGR."],
                    style_counts, cover_date, howto, n_inport=n_inport)

    def hdr_legend02(fig, right_px):
        x = right_px - 60
        _text(fig, x, 41, "Q1→Q5", 9, MUTE, ha="right")
        for i in range(4, -1, -1):
            _swatch(fig, x - 42 - (4 - i) * 12, 41.5, 9, 9, RAMP[i])

    _render_stacked_book(
        OUTPUT_DIR / f"별첨02_{_BM}_Sector_Quintile_Return_Book_{as_of}.pdf", records,
        f"{_BM} · Sector × Quintile Returns",
        "Appendix 02 — Sector Quintile Return Book", "Avg monthly return, %",
        cover02,
        lambda ax, rec, h: _chart_sector(ax, rec["sector_vals"], rec["sectors"],
                                         rec["dropped_idx"], h),
        "L–S CAGR", hdr_legend02)

    # ── 별첨03 ──
    def cover03(pp):
        def howto(fig, x, y):
            for i, (lab, c) in enumerate((("Long", LONG_C), ("Short", SHORT_C),
                                          ("Neutral", NEUT_C))):
                _swatch(fig, x + i * 92, y, 14, 14, c)
                _text(fig, x + i * 92 + 19, y + 1, lab, 11, SUB)
            _text(fig, x, y + 30, "Quintiles are selected as long or short by performance", 12, SUB)
            _text(fig, x, y + 48, "against a spread-based threshold — not fixed to Q1 / Q5.", 12, SUB)
            _text(fig, x, y + 66, "Grey quintiles are left out of the portfolio.", 12, SUB)
        _cover_page(pp, "03", ["Quintile", "Return Book"],
                    ["Average monthly return of each factor's quintile",
                     "portfolios and the long / short quintiles selected",
                     f"from them. {len(records)} factors, ordered by in-sample L–S CAGR."],
                    style_counts, cover_date, howto, n_inport=n_inport)

    _render_grid_book03(OUTPUT_DIR / f"별첨03_{_BM}_Quintile_Return_Book_{as_of}.pdf",
                        records, cover03)

    # ── 별첨04 ──
    def cover04(pp):
        def howto(fig, x, y):
            _text(fig, x, y, f"Each curve starts at 0 % in {start_label} and", 12, SUB)
            _text(fig, x, y + 18, "compounds monthly long–short returns. The line is", 12, SUB)
            _text(fig, x, y + 36, "coloured by the factor's style; the figure at the", 12, SUB)
            _text(fig, x, y + 54, "right edge is the period-end cumulative return.", 12, SUB)
        _cover_page(pp, "04", ["Long–Short Portfolio", "Return Book"],
                    ["Cumulative return of each factor's long–short",
                     f"portfolio, net of {cost_bps} bp transaction cost.",
                     f"{len(records)} factors over {n_months} months, ordered by in-sample CAGR."],
                    style_counts, cover_date, howto, n_inport=n_inport)

    _render_stacked_book(
        OUTPUT_DIR / f"별첨04_{_BM}_LongShort_Port_Return_Book_{as_of}.pdf", records,
        f"{_BM} · Long–Short Cumulative Returns",
        "Appendix 04 — Long–Short Portfolio Return Book",
        f"Cumulative return, % · net of {cost_bps} bp cost",
        cover04,
        lambda ax, rec, h: _chart_ls_series(ax, rec["series"], rec["color"], h),
        "CAGR")


if __name__ == "__main__":
    pass
