# -*- coding: utf-8 -*-
"""대시보드 조립 레이어.

기존 output CSV -> plotly 차트 -> 단일 자체완결 HTML 파일.
plotly.js 를 인라인으로 임베드해 오프라인에서도 단독으로 열린다.
파이프라인 코드는 건드리지 않는다(read-only).

진입점: build_dashboard(end_date=None) -> Path
"""
from __future__ import annotations

import logging
from pathlib import Path

from service.report import dashboard_charts as ch
from service.report import dashboard_data as dd

logger = logging.getLogger(__name__)

_PLOTLY_CFG = {"displayModeBar": False, "responsive": True}

_PAGE_CSS = """
:root { color-scheme: light; }
* { box-sizing: border-box; }
body { margin: 0; background: #f5f4ef; color: #2c2c2a;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Malgun Gothic', sans-serif; }
.wrap { max-width: 1200px; margin: 0 auto; padding: 24px 20px 48px; }
header { display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap;
  margin-bottom: 20px; }
header .title { font-size: 22px; font-weight: 500; }
header .date { font-size: 13px; color: #0c447c; background: #e6f1fb;
  padding: 3px 10px; border-radius: 8px; }
header .gen { font-size: 12px; color: #888780; margin-left: auto; }
h2 { font-size: 17px; font-weight: 500; color: #444441; margin: 28px 0 12px;
  border-bottom: 1px solid #d3d1c7; padding-bottom: 6px; }
.kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
  gap: 10px; margin-bottom: 14px; }
.kpi { background: #fff; border: 1px solid #e7e5dd; border-radius: 8px; padding: 10px 14px; }
.kpi-label { font-size: 12px; color: #5f5e5a; }
.kpi-val { font-size: 22px; font-weight: 500; margin-top: 2px; }
.grid2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
  gap: 14px; margin-bottom: 14px; }
.card { background: #fff; border: 1px solid #e7e5dd; border-radius: 12px;
  padding: 8px 10px; overflow: hidden; }
.card.full { margin-bottom: 14px; }
.note { font-size: 13px; color: #888780; padding: 8px 0; }
"""


def _fig_div(fig, include_js: bool = False) -> str:
    return fig.to_html(
        full_html=False,
        include_plotlyjs=("inline" if include_js else False),
        config=_PLOTLY_CFG,
    )


def _grid2(cards: list[str]) -> str:
    """카드 div 리스트를 2열 그리드 행들로 묶는다 (홀수는 마지막 단독)."""
    rows = []
    for i in range(0, len(cards), 2):
        rows.append('<div class="grid2">' + "".join(cards[i:i + 2]) + "</div>")
    return "".join(rows)


def _kpi_cards(kpis: dict) -> str:
    def fnum(v, fmt):
        try:
            return format(v, fmt)
        except (ValueError, TypeError):
            return "-"

    items = [
        ("CAGR", fnum(kpis["cagr"], ".2%")),
        ("MDD", fnum(kpis["mdd"], ".2%")),
        ("Sharpe", fnum(kpis["sharpe"], ".2f")),
        ("Calmar", fnum(kpis["calmar"], ".2f")),
        ("승률 (vs Top50)", fnum(kpis["win_rate"], ".1%")),
        ("Funnel", kpis.get("funnel_pattern") or "-"),
    ]
    return "".join(
        f'<div class="kpi"><div class="kpi-label">{lbl}</div>'
        f'<div class="kpi-val">{val}</div></div>'
        for lbl, val in items
    )


def _build_backtest_section(output_dir: Path) -> tuple[list[str], bool]:
    """백테스트 섹션 HTML 조각 리스트와 'plotly.js 포함 여부' 반환."""
    wf_path = output_dir / "walk_forward_results.csv"
    if not wf_path.exists():
        return (['<div class="note">walk_forward_results.csv 없음 - 백테스트 섹션 생략. '
                 '(python main.py backtest ... 실행 필요)</div>'], False)

    curves = dd.load_backtest_curves(wf_path)
    diag = dd.parse_diagnostics(output_dir / "overfit_diagnostics.csv")
    kpis = dd.build_kpis(curves, diag)

    start = curves.index.min().strftime("%Y-%m")
    end = curves.index.max().strftime("%Y-%m")
    parts = [
        f'<h2>1. 백테스트 ({start} ~ {end}, 월별 {kpis["n_months"]}개월)</h2>',
        f'<div class="kpi-grid">{_kpi_cards(kpis)}</div>',
        f'<div class="card full">{_fig_div(ch.equity_curve_fig(curves), include_js=True)}</div>',
    ]
    cards = [
        f'<div class="card">{_fig_div(ch.drawdown_fig(curves))}</div>',
        f'<div class="card">{_fig_div(ch.monthly_dist_fig(curves))}</div>',
    ]

    # 가중치 이력이 직렬화돼 있으면 스타일 비중 추이 + 회전율 추가 (백테스트 재실행 산출)
    wh_path = output_dir / "walk_forward_weight_history.csv"
    if wh_path.exists():
        wh = dd.load_weight_history(wh_path)
        fi_path = dd.DATA_DIR / "factor_info.csv"
        fmap = dd.factor_style_map(fi_path) if fi_path.exists() else {}
        style_hist = dd.style_weight_history(wh, fmap)
        turnover = dd.compute_turnover(wh)
        cards.append(f'<div class="card">{_fig_div(ch.style_weight_evolution_fig(style_hist))}</div>')
        cards.append(f'<div class="card">{_fig_div(ch.turnover_fig(turnover))}</div>')

    parts.append(_grid2(cards))
    return parts, True


def _build_portfolio_section(output_dir: Path, end_date: str | None,
                             js_already: bool, data_dir: Path) -> list[str]:
    weights_path = dd.find_latest_weights_file(output_dir, end_date)
    if weights_path is None:
        return ['<h2>2. 현재 포트 / 배팅</h2>'
                '<div class="note">total_aggregated_weights_*.csv 없음 - '
                '현재 포트 섹션 생략. (python main.py mp ... 실행 필요)</div>']

    weights = dd.load_weights(weights_path)
    snap = dd.snapshot_date_from_path(weights_path) or "?"
    style_w = dd.aggregate_style_weights(weights)
    ls_df = dd.top_longs_shorts(weights, n=15)
    tilt = dd.factor_tilt(weights)
    selected = dd.active_factors(weights)

    style_cap = 0.25
    benchmark = "MXCN1A"
    try:
        from config import PARAM, PIPELINE_PARAMS
        style_cap = float(PIPELINE_PARAMS.get("style_cap", 0.25))
        benchmark = PARAM.get("benchmark", "MXCN1A")
    except Exception:  # noqa: BLE001 - config 없어도 기본값으로 진행
        pass

    cards = [
        f'<div class="card">{_fig_div(ch.style_allocation_fig(style_w, style_cap), include_js=not js_already)}</div>'
    ]

    # 섹터 분해: 소스 parquet 을 read-only 로 읽어 gvkeyiid 로 join (파이프라인 무수정)
    if snap != "?":
        sector_map = dd.load_sector_map(data_dir, benchmark, snap)
        if sector_map:
            sec_series = dd.sector_net_weights(weights, sector_map)
            if not sec_series.empty:
                cards.append(f'<div class="card">{_fig_div(ch.sector_net_fig(sec_series))}</div>')

    cards.append(f'<div class="card">{_fig_div(ch.longs_shorts_fig(ls_df))}</div>')
    cards.append(f'<div class="card">{_fig_div(ch.factor_tilt_fig(tilt))}</div>')

    meta_path = output_dir / "meta_data.csv"
    if meta_path.exists():
        meta = dd.load_meta(meta_path)
        cards.append(f'<div class="card">{_fig_div(ch.leaderboard_fig(meta, selected))}</div>')

    deltas = dd.load_style_deltas(output_dir, snap)
    if deltas is not None and not deltas.empty:
        cards.append(f'<div class="card">{_fig_div(ch.style_delta_fig(deltas))}</div>')

    return [f'<h2>2. 현재 포트 / 배팅 (스냅샷 {snap})</h2>', _grid2(cards)]


def build_dashboard(end_date: str | None = None, output_dir: Path | None = None,
                    data_dir: Path | None = None) -> Path:
    """대시보드 HTML 을 생성해 output/dashboard_<date>.html 로 저장하고 경로 반환."""
    output_dir = Path(output_dir) if output_dir else dd.OUTPUT_DIR
    data_dir = Path(data_dir) if data_dir else dd.DATA_DIR

    bt_parts, js_in_bt = _build_backtest_section(output_dir)
    pf_parts = _build_portfolio_section(output_dir, end_date, js_already=js_in_bt, data_dir=data_dir)

    # 파일명용 스냅샷 날짜: 가중치 파일 -> 백테스트 마지막 -> 'latest'
    wfile = dd.find_latest_weights_file(output_dir, end_date)
    snap = dd.snapshot_date_from_path(wfile) if wfile else None
    if snap is None:
        wf = output_dir / "walk_forward_results.csv"
        if wf.exists():
            snap = dd.load_backtest_curves(wf).index.max().strftime("%Y-%m-%d")
    snap = snap or (end_date or "latest")

    header = (
        '<header>'
        '<span class="title">BOK 포트폴리오 대시보드</span>'
        f'<span class="date">{snap}</span>'
        '<span class="gen">read-only viz - 기존 output CSV 기반</span>'
        '</header>'
    )
    body = header + "".join(bt_parts) + "".join(pf_parts)
    html = (
        '<!DOCTYPE html><html lang="ko"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f'<title>BOK 포트폴리오 대시보드 {snap}</title>'
        f'<style>{_PAGE_CSS}</style></head><body>'
        f'<div class="wrap">{body}</div></body></html>'
    )

    out_path = output_dir / f"dashboard_{snap}.html"
    out_path.write_text(html, encoding="utf-8")
    logger.info("Dashboard saved to %s", out_path)
    return out_path
