# -*- coding: utf-8 -*-
"""대시보드 조립 레이어.

기존 output CSV -> plotly 차트 -> 단일 자체완결 HTML 파일.
plotly.js 를 인라인으로 임베드해 오프라인에서도 단독으로 열린다.
파이프라인 코드는 건드리지 않는다(read-only).

진입점: build_dashboard(end_date=None) -> Path
"""
from __future__ import annotations

import logging
from html import escape
from pathlib import Path

import pandas as pd

from service.report import dashboard_charts as ch
from service.report import dashboard_data as dd

logger = logging.getLogger(__name__)

_PLOTLY_CFG = {"displayModeBar": False, "responsive": True}

_PAGE_CSS = """
:root {
  --canvas:#0b0e11; --card:#1e2329; --elev:#2b3139; --hair:#2b3139;
  --primary:#fcd535; --on-primary:#181a20; --body:#eaecef; --on-dark:#ffffff;
  --muted:#707a8a; --muted-strong:#929aa5; --up:#0ecb81; --down:#f6465d;
  --sans:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Malgun Gothic',sans-serif;
  --mono:'JetBrains Mono','Consolas',ui-monospace,monospace;
  color-scheme: dark;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--canvas); color: var(--body);
  font-family: var(--sans); }
.wrap { max-width: 1200px; margin: 0 auto; padding: 24px 20px 48px; }
header { display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap;
  margin-bottom: 20px; }
header .title { font-size: 24px; font-weight: 600; letter-spacing: -0.3px; color: var(--on-dark); }
header .date { font-size: 13px; font-weight: 600; color: var(--primary); background: var(--card);
  padding: 4px 12px; border-radius: 8px; font-family: var(--mono); }
header .gen { font-size: 12px; color: var(--muted); margin-left: auto; }
h2 { font-size: 18px; font-weight: 600; color: var(--on-dark); margin: 32px 0 14px;
  border-bottom: 1px solid var(--hair); padding-bottom: 8px; }
.kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
  gap: 10px; margin-bottom: 14px; }
.kpi { background: var(--card); border: 1px solid var(--hair); border-radius: 8px; padding: 12px 14px; }
.kpi-label { font-size: 12px; color: var(--muted); }
.kpi-val { font-size: 22px; font-weight: 600; margin-top: 2px; color: var(--on-dark);
  font-family: var(--mono); letter-spacing: -0.3px; }
.grid2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
  gap: 14px; margin-bottom: 14px; }
.card { background: var(--card); border: 1px solid var(--hair); border-radius: 12px;
  padding: 8px 10px; overflow: hidden; }
.card.full { margin-bottom: 14px; }
.note { font-size: 13px; color: var(--muted); padding: 8px 0; }
.plotly-graph-div { width: 100% !important; }
.diag-table { width: 100%; border-collapse: collapse; font-size: 13px; }
.diag-table th { text-align: left; color: var(--muted); font-weight: 600; font-size: 12px;
  border-bottom: 1px solid var(--hair); padding: 8px 10px; }
.diag-table td { padding: 7px 10px; border-bottom: 1px solid var(--hair); vertical-align: top; }
.diag-table tbody tr:last-child td { border-bottom: none; }
.diag-table tr:hover td { background: var(--elev); }
.diag-cat { color: var(--primary); font-weight: 600; white-space: nowrap; }
.diag-val { font-family: var(--mono); color: var(--on-dark); white-space: nowrap; text-align: right; }
.diag-val.diag-span { text-align: center; color: var(--muted-strong); }
.diag-interp { color: var(--muted-strong); }
.dd-cap { font-size: 13px; font-weight: 600; color: var(--primary); padding: 2px 10px 10px; }
"""

# 일부 브라우저에서 plotly 초기 렌더 폭이 컨테이너와 어긋나 잘리는 경우 대비:
# 로드/리사이즈 시 각 차트를 컨테이너 폭에 맞춰 다시 그린다.
_RESIZE_SCRIPT = (
    "<script>(function(){function fit(){if(!window.Plotly)return;"
    "document.querySelectorAll('.plotly-graph-div').forEach(function(d){"
    "try{Plotly.Plots.resize(d);}catch(e){}});}"
    "window.addEventListener('load',fit);window.addEventListener('resize',fit);"
    "setTimeout(fit,300);})();</script>"
)


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


# EW_All / EW_Top50 / Constrained EW 접두 지표는 EW/Top50/CEW 3열로 피벗한다.
# (Funnel Value-Add 의 CAGR/MDD 비교 — funnel 의 핵심이라 나란히 보여 가독성↑)
_DIAG_VARIANTS = [("Constrained EW ", "cew"), ("EW_Top50 ", "top50"), ("EW_All ", "ew")]


def _fmt_perf(v: float, is_pct: bool) -> str:
    """성과 값 포맷 (nan -> '-', 비율 -> %, 그 외 소수 2자리)."""
    if v != v:  # nan
        return "-"
    return f"{v:.2%}" if is_pct else f"{v:.2f}"


def _oos_rows(curves) -> list[dict]:
    """OOS 성과(CAGR/MDD/Sharpe/Calmar)를 EW/Top50/CEW 3열로 비교하는 진단 표용 행 리스트.

    walk_forward_results.csv 의 곡선에서 직접 계산 — overfit_diagnostics.csv 와 동일 공식
    (research §7.3 검증)이라 진단값과 일치하며, CSV 엔 없는 EW_All/EW_Top50 의 Sharpe/Calmar
    까지 메운다. EW=EW_All(전체 유효 팩터 동일가중), Top50=Top-50 후보군 동일가중, CEW=최종 MP.
    곡선/컬럼이 없으면 빈 리스트(호출부는 CSV 원본 표기로 폴백).
    """
    if curves is None:
        return []
    specs = [("ew", "ew_all_return", "ew_all_cumulative"),
             ("top50", "ew_top50_return", "ew_top50_cumulative"),
             ("cew", "cew_return", "cew_cumulative")]
    perf = {}
    for key, rc, cc in specs:
        if rc not in curves.columns or cc not in curves.columns:
            return []
        perf[key] = dd.compute_series_perf(curves, rc, cc)

    metrics = [("CAGR", "cagr", True), ("MDD", "mdd", True),
               ("Sharpe", "sharpe", False), ("Calmar", "calmar", False)]
    return [
        {"cat": "OOS 성과 (EW/Top50/CEW)", "metric": label, "single": None, "interp": "",
         "ew": _fmt_perf(perf["ew"][m], pct),
         "top50": _fmt_perf(perf["top50"][m], pct),
         "cew": _fmt_perf(perf["cew"][m], pct)}
        for label, m, pct in metrics
    ]


_OOS_CSV_CATS = ("OOS 성과 - Constrained EW", "OOS 성과 - EW")


def _diagnostics_table(output_dir: Path, curves=None) -> str:
    """overfit_diagnostics.csv 전체를 분류별로 묶은 다크 테마 HTML 표로 렌더(read-only).

    parse_diagnostics 는 KPI 추출용이라 Interpretation/행순서를 버린다. 전체 표는
    CSV(세로형 Category/Metric/Value/Interpretation)를 직접 읽어 순서대로 렌더한다.
    그 외 단일값 행은 3열을 colspan 으로 합쳐 가운데 표시한다.
    Interpretation 에 '<','>' (예: 'A < B < C')가 섞이므로 셀은 모두 escape 한다.

    curves(walk_forward_results.csv)가 있으면 OOS 성과를 곡선에서 직접 계산한 단일
    "OOS 성과" 블록(CAGR/MDD/Sharpe/Calmar x EW/Top50/CEW)으로 통합한다. funnel 이
    이미 OOS CAGR/MDD 를 보여주므로 중복을 막고자, 이때 funnel 의 EW_All/EW_Top50/
    Constrained EW 변형행과 CSV OOS 섹션은 숨긴다(패턴 판정 행은 유지). 곡선이 없으면
    (test 모드 등) 변형행을 그대로 EW/Top50/CEW 로 피벗하는 폴백을 쓴다.
    """
    p = output_dir / "overfit_diagnostics.csv"
    if not p.exists():
        return ""
    df = pd.read_csv(p, encoding="utf-8-sig").fillna("")

    oos_rows = _oos_rows(curves)       # 곡선 기반 OOS 블록 (없으면 [])
    out_rows: list[dict] = []          # 순서 보존된 출력 행
    pivot_idx: dict[tuple, int] = {}   # (cat, base_metric) -> out_rows 인덱스
    oos_inserted = False
    for _, r in df.iterrows():
        cat = str(r["Category"]).strip()
        metric = str(r["Metric"]).strip()
        value = str(r["Value"]).strip()
        interp = str(r["Interpretation"]).strip()

        if oos_rows:
            # CSV OOS 섹션 위치에 곡선 기반 OOS 블록을 삽입하고 원본 행은 숨김
            if cat in _OOS_CSV_CATS:
                if not oos_inserted:
                    out_rows.extend(oos_rows)
                    oos_inserted = True
                continue
            # funnel 의 변형 CAGR/MDD 는 OOS 블록으로 흡수 -> 숨김 (패턴 행은 유지)
            if any(metric.startswith(pfx) for pfx, _ in _DIAG_VARIANTS):
                continue

        matched = next((v for v in _DIAG_VARIANTS if metric.startswith(v[0])), None)
        if matched:
            prefix, col = matched
            key = (cat, metric[len(prefix):].strip())  # base metric (CAGR/MDD)
            if key not in pivot_idx:
                pivot_idx[key] = len(out_rows)
                # 피벗 행은 EW/Top50/CEW 헤더가 자체 설명하므로 해석 생략
                out_rows.append({"cat": cat, "metric": key[1], "single": None,
                                 "ew": "", "top50": "", "cew": "", "interp": ""})
            out_rows[pivot_idx[key]][col] = value
        else:
            out_rows.append({"cat": cat, "metric": metric, "single": value, "interp": interp})

    if oos_rows and not oos_inserted:  # CSV 에 OOS 섹션이 없던 경우(구버전) 끝에 추가
        out_rows.extend(oos_rows)

    trs, last_cat = [], None
    for row in out_rows:
        cat_cell = escape(row["cat"]) if row["cat"] != last_cat else ""
        last_cat = row["cat"]
        if row["single"] is not None:
            val_cells = f'<td class="diag-val diag-span" colspan="3">{escape(row["single"])}</td>'
        else:
            val_cells = (
                f'<td class="diag-val">{escape(row["ew"])}</td>'
                f'<td class="diag-val">{escape(row["top50"])}</td>'
                f'<td class="diag-val">{escape(row["cew"])}</td>'
            )
        trs.append(
            f'<tr><td class="diag-cat">{cat_cell}</td>'
            f'<td>{escape(row["metric"])}</td>'
            f'{val_cells}'
            f'<td class="diag-interp">{escape(row["interp"])}</td></tr>'
        )
    return (
        '<div class="card full"><table class="diag-table">'
        '<thead><tr><th>분류</th><th>지표</th><th>EW</th><th>Top50</th><th>CEW</th><th>해석</th></tr></thead>'
        f'<tbody>{"".join(trs)}</tbody></table></div>'
    )


_DD_CURVE_SPECS = [("EW(전체)", "ew_all_cumulative"),
                   ("Top50", "ew_top50_cumulative"),
                   ("CEW(최종)", "cew_cumulative")]


def _dd_episode_row(e: dict) -> str:
    def ym(d):
        return d.strftime("%Y-%m") if d is not None else "ONGOING"

    def mo(v):
        return f"{v}m" if v is not None else "ONGOING"

    return (
        f'<tr><td class="diag-val">{e["depth"]:.2%}</td>'
        f'<td>{ym(e["peak"])}</td><td>{ym(e["trough"])}</td>'
        f'<td class="diag-val">{mo(e["peak_to_trough"])}</td>'
        f'<td>{ym(e["recovery"])}</td>'
        f'<td class="diag-val">{mo(e["trough_to_recovery"])}</td>'
        f'<td class="diag-val">{mo(e["total"])}</td></tr>'
    )


def _drawdown_episodes_section(curves) -> str:
    """EW/Top50/CEW 곡선별 낙폭 episode(1% 이상) 표를 묶어 반환(곡선 없으면 생략)."""
    blocks = []
    for label, cum_col in _DD_CURVE_SPECS:
        if cum_col not in curves.columns:
            continue
        eps = dd.compute_drawdown_episodes(curves[cum_col], min_depth=0.01)
        if not eps:
            continue
        mdd = min(e["depth"] for e in eps)
        rows = "".join(_dd_episode_row(e) for e in eps)
        blocks.append(
            '<div class="card full">'
            f'<div class="dd-cap">{escape(label)} - {len(eps)} episodes, MDD {mdd:.2%}</div>'
            '<table class="diag-table"><thead><tr>'
            '<th>DD</th><th>peak</th><th>trough</th><th>peak→trough</th>'
            '<th>recovery</th><th>trough→recovery</th><th>total</th>'
            '</tr></thead><tbody>'
            f'{rows}</tbody></table></div>'
        )
    return "".join(blocks)


def _benchmark() -> str:
    """유니버스명 (config PARAM). 미가용 시 'BOK'."""
    try:
        from config import PARAM
        return PARAM["benchmark"]
    except Exception:
        return "BOK"


def _is_label() -> tuple[str, str]:
    """학습(IS) 표기 (제목용, 본문용). 롤링 창이면 '롤링 N개월', 아니면
    'YYYY-MM부터 확장창' (config backtest_start). config 미가용 시 빈 문자열."""
    try:
        from config import PIPELINE_PARAMS
        w = PIPELINE_PARAMS.get("is_window_months")
        if w:
            return f"학습 롤링 {int(w)}개월", f"학습(IS) 롤링 {int(w)}개월 창"
        start = pd.Timestamp(PIPELINE_PARAMS["backtest_start"]).strftime("%Y-%m")
        return f"학습 {start}~ 확장창", f"학습(IS) {start}부터 확장창"
    except Exception:
        return "", ""


def _backtest_stats_card(curves) -> str:
    """확장 성과 통계(QC Key Statistics 스타일) + 벤치마크(선정 EW) 대비 지표를 KPI 그리드로."""
    s = dd.extended_stats(curves)
    rel = dd.relative_metrics(curves, bench_col="ew_return")

    def pct(v):
        return "-" if v != v else f"{v:.2%}"

    def dec(v):
        return "-" if v != v else f"{v:.2f}"

    items = [
        ("연환산 변동성", pct(s["ann_vol"])),
        ("Sortino", dec(s["sortino"])),
        ("최고 월", pct(s["best_month"])),
        ("최저 월", pct(s["worst_month"])),
        ("상승월 비율", pct(s["pct_positive"])),
        ("평균 월수익", pct(s["avg_month"])),
        ("월수익 왜도", dec(s["skew"])),
        ("최장 연속손실", f'{s["max_loss_streak"]}M'),
    ]
    if rel:
        items += [
            ("Beta (vs 선정EW)", dec(rel["beta"])),
            ("Alpha 연 (vs 선정EW)", pct(rel["alpha_ann"])),
            ("정보비율", dec(rel["info_ratio"])),
            ("추적오차 연", pct(rel["tracking_error"])),
        ]
    cells = "".join(
        f'<div class="kpi"><div class="kpi-label">{escape(lbl)}</div>'
        f'<div class="kpi-val">{escape(val)}</div></div>'
        for lbl, val in items
    )
    return f'<div class="kpi-grid">{cells}</div>'


def _vol_regime_note(summary: dict) -> str:
    """변동성 국면 요약 텍스트 - Bloomberg multiplier/TE 타깃 정성 참고용 설명."""
    text = (
        f'현재 실현변동성 {summary["realized_vol"]:.1%} '
        f'(역대 {summary["percentile"]:.0%} 백분위, '
        f'레인지 {summary["min_vol"]:.1%}~{summary["max_vol"]:.1%}), '
        f'중위 {summary["median_vol"]:.1%}, '
        f'참고 배수 k={summary["k"]:.2f} (cap {summary["k_cap"]:.1f}). '
        'Bloomberg multiplier/TE 타깃 정할 때 정성 참고용 - 예: 평소 TE 타깃 x k'
    )
    return escape(text)


def _vol_regime_section(curves) -> str:
    """변동성 국면 차트 + 요약 카드 (행 부족 등으로 계산 불가하면 빈 문자열 - 섹션 생략)."""
    result = dd.build_vol_regime(curves)
    if result is None:
        return ""
    vr_df, summary = result
    return (
        f'<div class="card full">{_fig_div(ch.build_vol_regime_chart(vr_df))}</div>'
        f'<div class="card full"><div class="note">{_vol_regime_note(summary)}</div></div>'
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
    is_title, is_body = _is_label()
    title_range = f"{is_title} · OOS {start}~{end}" if is_title else f"OOS {start}~{end}"
    is_note = (f"{is_body} · 성과는 OOS {start}~ 집계 · " if is_body else "")
    parts = [
        f'<h2>1. 백테스트 ({title_range}, OOS {kpis["n_months"]}개월)</h2>',
        f'<div class="kpi-grid">{_kpi_cards(kpis)}</div>',
        f'<div class="note">{is_note}상세 통계/벤치마크 = 선정 EW(1/N)</div>',
        _backtest_stats_card(curves),
        f'<div class="card full">{_fig_div(ch.equity_curve_fig(curves), include_js=True)}</div>',
    ]
    # 월별 수익률 히트맵 (QC 스타일 연x월 그리드)
    mret = dd.monthly_returns_table(curves)
    if not mret.empty:
        parts.append(f'<div class="card full">{_fig_div(ch.monthly_returns_heatmap_fig(mret))}</div>')
    parts.append(_grid2([
        f'<div class="card">{_fig_div(ch.drawdown_fig(curves))}</div>',
        f'<div class="card">{_fig_div(ch.monthly_dist_fig(curves))}</div>',
    ]))
    # 롤링 12개월 Sharpe (>=12개월 구간에서만)
    rs = dd.rolling_sharpe(curves)
    if not rs.empty:
        parts.append(f'<div class="card full">{_fig_div(ch.rolling_sharpe_fig(rs))}</div>')

    # 가중치 이력이 직렬화돼 있으면 스타일 비중 추이 + 회전율 추가 (백테스트 재실행 산출).
    # 스타일 추이는 범례(스타일 7~8개)가 넓어 풀폭 카드로 둔다 - 반폭이면 범례가 그래프 침범.
    wh_path = output_dir / "walk_forward_weight_history.csv"
    if wh_path.exists():
        wh = dd.load_weight_history(wh_path)
        fi_path = dd.DATA_DIR / "factor_info.csv"
        fmap = dd.factor_style_map(fi_path) if fi_path.exists() else {}
        style_hist = dd.style_weight_history(wh, fmap)
        turnover = dd.compute_turnover(wh)
        churn_split = dd.selection_churn_split(wh)
        parts.append(f'<div class="card full">{_fig_div(ch.style_weight_evolution_fig(style_hist))}</div>')
        parts.append(f'<div class="card full">{_fig_div(ch.turnover_fig(turnover, churn_split))}</div>')

    vol_regime_section = _vol_regime_section(curves)
    if vol_regime_section:
        parts.append('<h2>변동성 국면 (multiplier 참고)</h2>')
        parts.append(vol_regime_section)

    diag_tbl = _diagnostics_table(output_dir, curves)
    if diag_tbl:
        parts.append('<h2>과적합 진단 상세</h2>')
        parts.append(diag_tbl)

    dd_section = _drawdown_episodes_section(curves)
    if dd_section:
        parts.append('<h2>낙폭 구간 분석 (1% 이상 episode, 깊은 순)</h2>')
        parts.append(dd_section)

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
    deltas = dd.load_style_deltas(output_dir, snap)
    style_w = dd.style_allocation(weights, deltas)
    ls_df = dd.top_longs_shorts(weights, n=15)
    tilt = dd.factor_tilt(weights)
    selected = dd.active_factors(weights)

    style_cap = 0.25
    benchmark = "MXCN1A"
    try:
        from config import PARAM, PIPELINE_PARAMS
        style_cap = float(PIPELINE_PARAMS.get("style_cap", 0.25))
        benchmark = PARAM.get("benchmark", "MXCN1A")
    except (ImportError, AttributeError):  # config 없어도 기본값으로 진행 (그 외 오류는 전파)
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

    ls_decomp = dd.longs_shorts_style_decomposition(weights, n=15)
    cards.append(f'<div class="card">{_fig_div(ch.longs_shorts_fig(ls_df, ls_decomp))}</div>')
    cards.append(f'<div class="card">{_fig_div(ch.factor_tilt_fig(tilt))}</div>')

    meta_path = output_dir / "meta_data.csv"
    if meta_path.exists():
        meta = dd.load_meta(meta_path)
        cards.append(f'<div class="card">{_fig_div(ch.leaderboard_fig(meta, selected, tilt))}</div>')

    if deltas is not None and not deltas.empty:
        cards.append(f'<div class="card">{_fig_div(ch.style_delta_fig(deltas))}</div>')

    parts = [f'<h2>2. 현재 포트 / 배팅 (스냅샷 {snap})</h2>', _grid2(cards)]

    clusters_html = _factor_clusters_section(output_dir, snap)
    if clusters_html:
        parts.append(clusters_html)
    cap_html = _style_cap_section(output_dir, snap)
    if cap_html:
        parts.append(cap_html)
    regime_html = _correlation_regime_section(output_dir)
    if regime_html:
        parts.append(regime_html)
    return parts


def _correlation_regime_section(output_dir: Path) -> str:
    """상관 국면 참고 섹션 (multiplier 참고용 — 자동 스케일링에 미사용).

    factor_returns_matrix.csv (walk-forward 저장) 기반:
      - 평균 쌍상관: rolling 12M 팩터 간 평균 상관 (급등 = 매크로 쏠림 장세)
      - 흡수률: rolling 12M cov 상위 5 고유값의 분산 설명 비중 (Kritzman 계열)
    CEW 연수익 음수인 해는 음영 처리해 국면 지표와 죽은 해의 겹침을 보여준다.
    """
    path = output_dir / "factor_returns_matrix.csv"
    if not path.exists():
        return ""
    rets = pd.read_csv(path, index_col=0, parse_dates=True)
    rets = rets.iloc[1:]  # 기준점 0 행 제외
    if len(rets) < 24:
        return ""

    import numpy as np
    win = 12
    dates, mean_corr, absorption = [], [], []
    for i in range(win, len(rets) + 1):
        w = rets.iloc[i - win:i]
        w = w.loc[:, w.notna().all() & (w.std() > 0)]
        if w.shape[1] < 10:
            continue
        c = np.corrcoef(w.to_numpy(), rowvar=False)
        n = c.shape[0]
        mean_corr.append((c.sum() - n) / (n * (n - 1)))
        ev = np.linalg.eigvalsh(np.cov(w.to_numpy(), rowvar=False))
        absorption.append(float(ev[-5:].sum() / ev.sum()))
        dates.append(w.index[-1])
    if len(dates) < 12:
        return ""

    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.06,
                        row_heights=[0.62, 0.38],
                        specs=[[{"secondary_y": True}], [{}]])
    fig.add_trace(go.Scatter(x=dates, y=mean_corr, name="평균 쌍상관 (12M)",
                             line=dict(color="#5B8DEF", width=2)),
                  row=1, col=1, secondary_y=False)
    fig.add_trace(go.Scatter(x=dates, y=absorption, name="흡수률 (상위 5 고유값, 12M)",
                             line=dict(color="#E8944A", width=2)),
                  row=1, col=1, secondary_y=True)

    # 하단: 월별 전략(CEW) 수익률 바 + 연수익 음수 해 음영 (두 행 공통)
    wf = output_dir / "walk_forward_results.csv"
    if wf.exists():
        cew = pd.read_csv(wf, index_col=0, parse_dates=True)["cew_return"].dropna()
        fig.add_trace(go.Bar(
            x=cew.index, y=cew * 100, name="CEW 월수익률(%)",
            marker_color=["#4FBF87" if v >= 0 else "#E06C75" for v in cew],
        ), row=2, col=1)
        yearly = cew.groupby(cew.index.year).apply(lambda x: (1 + x).prod() - 1)
        for yr, r in yearly.items():
            if r < 0:
                fig.add_vrect(x0=f"{yr}-01-01", x1=f"{yr}-12-31",
                              fillcolor="#E06C75", opacity=0.07, line_width=0,
                              row="all", col=1)

    fig.update_layout(template="plotly_dark", height=460,
                      margin=dict(l=40, r=40, t=30, b=30),
                      legend=dict(orientation="h", y=1.1), showlegend=True,
                      bargap=0.15,
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    fig.update_yaxes(title_text="평균 상관", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="흡수률", row=1, col=1, secondary_y=True)
    fig.update_yaxes(title_text="월수익률 %", row=2, col=1)

    note = ('<div class="note">multiplier 참고 지표 (자동 스케일링 미사용 — 변동성 국면 섹션과 동일 지위). '
            '상단: 평균 상관·흡수률 급등 = 팩터가 한 방향으로 쓸리는 매크로 장세로 L/S 분산 효과 약화. '
            '하단: CEW 월수익률 (OOS 시작 2018-06 이후). 붉은 음영 = CEW 연수익 음수인 해. '
            '데이터: walk-forward 전기간 팩터 수익률 (마지막 IS 규칙 기준, 상관 구조 참고용).</div>')
    return f'<h2>상관 국면 (multiplier 참고)</h2>{note}<div class="card">{_fig_div(fig)}</div>'


def _style_cap_section(output_dir: Path, snap: str) -> str:
    """스타일 캡 25% 적용 전/후 스타일 배분 비교 (mp 저장 style_cap_effect CSV 기반)."""
    path = output_dir / "mp_weight_history" / f"style_cap_effect_{snap}.csv"
    if not path.exists():
        return ""
    df = pd.read_csv(path)
    if df.empty or (df["raw_weight"] - df["fitted_weight"]).abs().max() < 1e-12:
        return ""  # 캡 미발동(전후 동일)이면 생략

    style_cap = 0.25
    try:
        from config import PIPELINE_PARAMS
        style_cap = float(PIPELINE_PARAMS.get("style_cap", 0.25))
    except (ImportError, AttributeError):
        pass

    g = df.groupby("styleName")[["raw_weight", "fitted_weight"]].sum()
    g = g.sort_values("raw_weight", ascending=False)
    scale = max(g["raw_weight"].max(), g["fitted_weight"].max(), style_cap) * 1.15

    rows = []
    for style, r in g.iterrows():
        pre, post = r["raw_weight"], r["fitted_weight"]
        delta = post - pre
        capped = pre > style_cap + 1e-9
        tag = ('<span class="capchip cut">캡 발동</span>' if capped
               else ('<span class="capchip up">재분배 수혜</span>' if delta > 1e-9 else ""))
        rows.append(
            f'<tr><td>{style} {tag}</td>'
            f'<td class="num">{pre*100:.1f}%</td>'
            f'<td class="capbars">'
            f'<div class="pre" style="width:{pre/scale*100:.1f}%"></div>'
            f'<div class="post" style="width:{post/scale*100:.1f}%"></div>'
            f'<div class="capline" style="left:{style_cap/scale*100:.1f}%"></div></td>'
            f'<td class="num">{post*100:.1f}%</td>'
            f'<td class="num" style="color:{"#E06C75" if delta < -1e-9 else "#4FBF87" if delta > 1e-9 else "inherit"}">'
            f'{delta*100:+.1f}%p</td></tr>'
        )

    css = (
        '<style>.cap-table{width:100%;border-collapse:collapse;font-size:13px;margin-top:8px}'
        '.cap-table th{text-align:left;opacity:.6;font-weight:500;padding:4px 8px}'
        '.cap-table td{padding:5px 8px;border-top:1px solid var(--border,#333)}'
        '.cap-table .num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}'
        '.capbars{position:relative;width:42%;height:20px}'
        '.capbars .pre{position:absolute;top:2px;height:7px;background:#5B8DEF55;border-radius:3px}'
        '.capbars .post{position:absolute;bottom:2px;height:7px;background:#5B8DEF;border-radius:3px}'
        '.capbars .capline{position:absolute;top:0;bottom:0;width:2px;background:#E06C75;opacity:.8}'
        '.capchip{padding:1px 6px;border-radius:8px;font-size:10px;font-weight:600;margin-left:4px}'
        '.capchip.cut{background:#E06C7522;color:#E06C75}'
        '.capchip.up{background:#4FBF8722;color:#4FBF87}</style>'
    )
    note = (f'<div class="note">위 바(연한색)=캡 적용 전 ERC 원비중, 아래 바(진한색)=캡 적용 후, '
            f'빨간 선=캡 {style_cap*100:.0f}%. 캡 초과 스타일의 초과분이 나머지 스타일로 재분배된다.</div>')
    header = ('<tr><th>스타일</th><th style="text-align:right">캡 전</th><th></th>'
              '<th style="text-align:right">캡 후</th><th style="text-align:right">변화</th></tr>')
    return (f'<h2>4. 스타일 캡 {style_cap*100:.0f}% 적용 전/후</h2>{css}{note}'
            f'<table class="cap-table"><thead>{header}</thead><tbody>{"".join(rows)}</tbody></table>')


def _factor_clusters_section(output_dir: Path, snap: str) -> str:
    """ERC 상관 무리 섹션: 어떤 팩터들이 한 배팅으로 묶였고 예산을 어떻게 나눴는지.

    mp 가 저장한 mp_weight_history/factor_clusters_{snap}.csv 기반 (없으면 생략).
    """
    path = output_dir / "mp_weight_history" / f"factor_clusters_{snap}.csv"
    if not path.exists():
        return ""
    df = pd.read_csv(path)
    if df.empty:
        return ""

    style_colors = {}
    palette = ["#5B8DEF", "#E8944A", "#4FBF87", "#C77DDA", "#E06C75", "#56B6C2",
               "#D19A66", "#98C379", "#B48EAD", "#7A869A"]
    for i, s in enumerate(df["styleName"].unique()):
        style_colors[s] = palette[i % len(palette)]

    groups = []
    for cid, g in df.groupby("cluster_id"):
        total_w = g["weight"].sum()
        n = len(g)
        title = (f'무리 {cid} · {n}개 팩터 · 합산 비중 {total_w*100:.1f}%'
                 if n > 1 else f'독립 · 비중 {total_w*100:.1f}%')
        avg_c = g["avg_corr_in_cluster"].replace("", pd.NA).dropna()
        if n > 1 and len(avg_c):
            title += f' · 무리 내 평균상관 {pd.to_numeric(avg_c).mean():.2f}'
        rows = "".join(
            f'<tr><td class="mono">{r.factor}</td>'
            f'<td><span class="chip" style="background:{style_colors.get(r.styleName, "#888")}22;'
            f'color:{style_colors.get(r.styleName, "#888")}">{r.styleName}</span></td>'
            f'<td class="num">{r.weight*100:.2f}%</td>'
            f'<td class="bar"><div style="width:{min(r.weight*100/0.07, 100):.0f}%;'
            f'background:{style_colors.get(r.styleName, "#888")}"></div></td></tr>'
            for r in g.itertuples()
        )
        groups.append(
            f'<details {"open" if n > 1 else ""} class="cluster"><summary>{title}</summary>'
            f'<table class="cl-table"><thead><tr><th>팩터</th><th>스타일</th>'
            f'<th>ERC 비중</th><th></th></tr></thead><tbody>{rows}</tbody></table></details>'
        )

    n_multi = int((df.groupby("cluster_id").size() > 1).sum())
    css = (
        '<style>.cluster{margin:8px 0;border:1px solid var(--border,#333);border-radius:8px;'
        'padding:6px 12px}.cluster summary{cursor:pointer;font-weight:600;padding:4px 0}'
        '.cl-table{width:100%;border-collapse:collapse;font-size:13px}'
        '.cl-table th{text-align:left;opacity:.6;font-weight:500;padding:2px 8px}'
        '.cl-table td{padding:3px 8px}.cl-table .num{text-align:right;font-variant-numeric:tabular-nums}'
        '.cl-table .mono{font-family:"JetBrains Mono",monospace;font-size:12px}'
        '.cl-table .bar{width:30%}.cl-table .bar div{height:8px;border-radius:4px}'
        '.chip{padding:1px 8px;border-radius:10px;font-size:11px;font-weight:600}</style>'
    )
    note = (f'<div class="note">|상관| &gt; 0.5 인 팩터끼리 한 무리로 묶음 (표시용 계층 클러스터링). '
            f'ERC 는 무리 전체가 한 배팅처럼 리스크 예산을 나눠 갖도록 개별 비중을 조정한다 — '
            f'무리 {n_multi}개 + 독립 팩터. 합산 비중이 큰 무리 순.</div>')
    return (f'<h2>3. ERC 상관 무리 (어떤 팩터가 한 배팅으로 묶였나)</h2>'
            f'{css}{note}{"".join(groups)}')


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
        f'<span class="title">{_benchmark()} 유니버스 전략 포트폴리오 대시보드</span>'
        f'<span class="date">{snap}</span>'
        '<span class="gen">read-only viz - 기존 output CSV 기반</span>'
        '</header>'
    )
    body = header + "".join(bt_parts) + "".join(pf_parts)
    html = (
        '<!DOCTYPE html><html lang="ko"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f'<title>{_benchmark()} 유니버스 전략 포트폴리오 대시보드 {snap}</title>'
        '<link rel="preconnect" href="https://fonts.googleapis.com">'
        '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
        '<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700'
        '&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">'
        f'<style>{_PAGE_CSS}</style></head><body>'
        f'<div class="wrap">{body}</div>{_RESIZE_SCRIPT}</body></html>'
    )

    out_path = output_dir / f"dashboard_{snap}.html"
    out_path.write_text(html, encoding="utf-8")
    logger.info("Dashboard saved to %s", out_path)
    return out_path
