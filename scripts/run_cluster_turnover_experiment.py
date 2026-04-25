# -*- coding: utf-8 -*-
"""Hierarchical Clustering x Turnover Smoothing 실험 러너.

8 케이스 그리드 (use_cluster_dedup x n_clusters x turnover_alpha) 를
ProcessPoolExecutor 로 병렬 실행하고, 케이스별 결과 CSV/JSON 및
요약 REPORT.md 를 생성한다.

사용법:
    python scripts/run_cluster_turnover_experiment.py --workers 4
    python scripts/run_cluster_turnover_experiment.py --sequential  # 순차 실행
    python scripts/run_cluster_turnover_experiment.py --start 2009-12-31 --end 2026-03-31
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import subprocess
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

logger = logging.getLogger(__name__)


# summary.csv 의 성과 관련 컬럼 (FAILED 행에서 NaN 으로 채움)
_PERF_COLUMNS: tuple[str, ...] = (
    "cagr_cew", "net_cagr_cew", "sharpe_cew", "mdd_cew", "calmar_cew",
    "cagr_ew", "sharpe_ew",
    "funnel_a_cagr", "funnel_b_cagr", "funnel_c_cagr",
    "oos_pctile_value", "strict_jaccard", "is_oos_rank_corr", "deflation_ratio",
)


def build_cases() -> list[dict[str, Any]]:
    """실험 11 케이스 정의.

    per_cluster_keep=3 고정, 한 축씩 변화시켜 효과를 분리한다.
    `*_nocap` 케이스는 style_cap=1.0 으로 스타일 캡을 사실상 해제
    (clustering 이 이미 다각화를 제공하므로 중복 제거 효과 검증).

    Returns:
        각 케이스는 {"name": str, "override": dict, "alpha": float} 형식.
    """
    return [
        {"name": "baseline", "override": {}, "alpha": 1.0},
        {"name": "cluster_18", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
        }, "alpha": 1.0},
        {"name": "cluster_12", "override": {
            "use_cluster_dedup": True, "n_clusters": 12, "per_cluster_keep": 3,
        }, "alpha": 1.0},
        {"name": "cluster_24", "override": {
            "use_cluster_dedup": True, "n_clusters": 24, "per_cluster_keep": 3,
        }, "alpha": 1.0},
        {"name": "smooth_0.7", "override": {}, "alpha": 0.7},
        {"name": "smooth_0.5", "override": {}, "alpha": 0.5},
        {"name": "combo_18_0.7", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
        }, "alpha": 0.7},
        {"name": "combo_18_0.5", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
        }, "alpha": 0.5},
        # style_cap 해제 변형 (clustering 이 이미 다각화를 제공한다는 가설 검증)
        {"name": "baseline_nocap", "override": {"style_cap": 1.0}, "alpha": 1.0},
        {"name": "cluster_nocap", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
            "style_cap": 1.0,
        }, "alpha": 1.0},
        {"name": "combo_nocap_0.5", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
            "style_cap": 1.0,
        }, "alpha": 0.5},
        # n_clusters 와이드 sweep (per_cluster_keep=3 고정)
        {"name": "cluster_8", "override": {
            "use_cluster_dedup": True, "n_clusters": 8, "per_cluster_keep": 3,
        }, "alpha": 1.0},
        {"name": "cluster_15", "override": {
            "use_cluster_dedup": True, "n_clusters": 15, "per_cluster_keep": 3,
        }, "alpha": 1.0},
        {"name": "cluster_20", "override": {
            "use_cluster_dedup": True, "n_clusters": 20, "per_cluster_keep": 3,
        }, "alpha": 1.0},
        {"name": "cluster_30", "override": {
            "use_cluster_dedup": True, "n_clusters": 30, "per_cluster_keep": 3,
        }, "alpha": 1.0},
        {"name": "cluster_40", "override": {
            "use_cluster_dedup": True, "n_clusters": 40, "per_cluster_keep": 3,
        }, "alpha": 1.0},
        # per_cluster_keep 변화 (n_clusters=18 고정)
        {"name": "cluster_18_keep1", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 1,
        }, "alpha": 1.0},
        {"name": "cluster_18_keep2", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 2,
        }, "alpha": 1.0},
        {"name": "cluster_18_keep5", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 5,
        }, "alpha": 1.0},
        # 중간 style_cap + 더 강한 smoothing
        {"name": "combo_18_cap0.5", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
            "style_cap": 0.5,
        }, "alpha": 0.5},
        {"name": "combo_18_0.3", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
        }, "alpha": 0.3},
        # Phase 3: 더 강한 smoothing alpha + nocap x smoothing 조합
        {"name": "combo_18_0.2", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
        }, "alpha": 0.2},
        {"name": "combo_18_0.1", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
        }, "alpha": 0.1},
        {"name": "baseline_nocap_0.5", "override": {"style_cap": 1.0}, "alpha": 0.5},
        {"name": "baseline_nocap_0.3", "override": {"style_cap": 1.0}, "alpha": 0.3},
        {"name": "combo_nocap_0.3", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
            "style_cap": 1.0,
        }, "alpha": 0.3},
        {"name": "combo_nocap_0.1", "override": {
            "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
            "style_cap": 1.0,
        }, "alpha": 0.1},
        # Phase 3: n_clusters verdict 경계 (8↔12 사이)
        {"name": "cluster_10", "override": {
            "use_cluster_dedup": True, "n_clusters": 10, "per_cluster_keep": 3,
        }, "alpha": 1.0},
    ]


def compute_avg_turnover(weight_history: pd.DataFrame) -> float:
    """Tier 2 리밸런싱 간 factor-level turnover 평균.

    turnover_t = (1/2) * sum_i |w_{t,i} - w_{t-1,i}|

    신규/사라진 팩터의 이전/현재 가중치는 0 으로 간주 (fillna(0)).

    Args:
        weight_history: index=rebal date, columns=factor names, values=weight.

    Returns:
        평균 turnover (0.0 ~ 1.0 범위). 리밸런싱이 2회 미만이면 NaN.
    """
    if weight_history is None or weight_history.empty or len(weight_history) < 2:
        return float("nan")

    wh = weight_history.fillna(0.0)
    # min_count=1 로 전체 NaN 행(diff 첫 행) 은 NaN 유지 → dropna 로 제거
    diffs = wh.diff().abs().sum(axis=1, min_count=1) / 2.0
    return float(diffs.dropna().mean())


def classify_verdict(funnel_pattern: str, oos_pctile: float) -> str:
    """Funnel pattern + OOS Percentile 로 종합 verdict 산출.

    spec §5.2 규칙:
      FILTER_OVERFIT (pattern) -> FILTER_OVERFIT
      OPTIMIZATION_OVERFIT (pattern) -> OPTIMIZATION_OVERFIT
      UNCATEGORIZED (pattern) -> UNCATEGORIZED
      INSUFFICIENT_DATA (pattern) -> N/A
      NORMAL + pctile >= 0.60 -> PERCENTILE_WARN
      NORMAL + pctile < 0.60 (또는 NaN) -> OK

    Args:
        funnel_pattern: `generate_overfit_report` 결과의 `funnel_pattern`.
        oos_pctile: 평균 OOS 백분위 (0~1). NaN 허용.

    Returns:
        verdict 문자열.
    """
    if funnel_pattern == "FILTER_OVERFIT":
        return "FILTER_OVERFIT"
    if funnel_pattern == "OPTIMIZATION_OVERFIT":
        return "OPTIMIZATION_OVERFIT"
    if funnel_pattern == "UNCATEGORIZED":
        return "UNCATEGORIZED"
    if funnel_pattern == "INSUFFICIENT_DATA":
        return "N/A"
    # NORMAL (또는 알 수 없는 값은 NORMAL 로 간주)
    if not pd.isna(oos_pctile) and oos_pctile >= 0.60:
        return "PERCENTILE_WARN"
    return "OK"


def build_summary_row(
    case: dict[str, Any],
    overfit_report: dict[str, Any] | None,
    avg_turnover: float,
    runtime_sec: float,
    status: str,
    error: str | None,
    tc_annual_rate: float = 0.012,  # 30bp x 4 rebal/year (default)
) -> dict[str, Any]:
    """케이스 1개의 결과를 summary.csv 한 행 dict 로 변환.

    `overfit_report` 가 None 이면 FAILED 케이스.

    tc_annual_rate: 팩터 간 리밸런싱 연간 거래비용 계수 (기본 0.012 = 30bp x 4/year).
        net_cagr_cew = cagr_cew - avg_turnover * tc_annual_rate.
        가중치 리밸런싱 주기/거래비용이 다르면 호출부에서 조정.
    """
    override = case.get("override", {})
    row: dict[str, Any] = {
        "case": case["name"],
        "use_cluster_dedup": bool(override.get("use_cluster_dedup", False)),
        "n_clusters": override.get("n_clusters"),
        "per_cluster_keep": override.get("per_cluster_keep"),
        "turnover_alpha": case["alpha"],
        "status": status,
        "error": error,
        "runtime_sec": runtime_sec,
        "avg_turnover": avg_turnover,
    }

    if status != "OK" or overfit_report is None:
        # FAILED: 성과 컬럼 NaN
        for k in _PERF_COLUMNS:
            row[k] = float("nan")
        row["funnel_verdict"] = "N/A"
        row["oos_pctile_flag"] = "N/A"
        row["verdict"] = "N/A"
        return row

    pattern = overfit_report["funnel_pattern"]
    pctile = overfit_report.get("oos_avg_percentile", float("nan"))

    row.update({
        "cagr_cew": overfit_report["oos_cagr"],
        "net_cagr_cew": (
            overfit_report["oos_cagr"]
            if pd.isna(avg_turnover)
            else overfit_report["oos_cagr"] - avg_turnover * tc_annual_rate
        ),
        "sharpe_cew": overfit_report["oos_sharpe"],
        "mdd_cew": overfit_report["oos_mdd"],
        "calmar_cew": overfit_report["oos_calmar"],
        "cagr_ew": overfit_report["oos_ew_cagr"],
        "sharpe_ew": overfit_report["oos_ew_sharpe"],
        "funnel_a_cagr": overfit_report["funnel_ew_all_cagr"],
        "funnel_b_cagr": overfit_report["funnel_ew_top50_cagr"],
        "funnel_c_cagr": overfit_report["funnel_cew_cagr"],
        "oos_pctile_value": pctile,
        "strict_jaccard": overfit_report.get("strict_jaccard", float("nan")),
        "is_oos_rank_corr": overfit_report.get("is_oos_rank_spearman", float("nan")),
        "deflation_ratio": overfit_report.get("deflation_ratio", float("nan")),
    })

    # funnel_verdict 라벨
    funnel_label_map = {
        "NORMAL": "OK (C>B>A)",
        "OPTIMIZATION_OVERFIT": "OPT_OVERFIT (B>C>A)",
        "FILTER_OVERFIT": "FILTER_OVERFIT (A>B)",
        "UNCATEGORIZED": "UNCATEGORIZED",
        "INSUFFICIENT_DATA": "N/A",
    }
    row["funnel_verdict"] = funnel_label_map.get(pattern, pattern)

    # oos_pctile_flag
    if pd.isna(pctile):
        row["oos_pctile_flag"] = "N/A"
    elif pctile >= 0.60:
        row["oos_pctile_flag"] = "WARN"
    else:
        row["oos_pctile_flag"] = "OK"

    # 종합 verdict
    row["verdict"] = classify_verdict(pattern, pctile)

    return row


def run_single_case(
    case: dict[str, Any],
    out_root: str,
    common: dict[str, Any],
) -> dict[str, Any]:
    """단일 케이스를 실행하고 summary row dict 를 반환.

    워커 프로세스 내부에서 호출된다. 예외 포집으로 실패 케이스도
    FAILED 행으로 반환되어 병렬 실행이 한 케이스 때문에 중단되지 않는다.

    Args:
        case: build_cases() 의 단일 항목.
        out_root: 실험 결과 루트 디렉토리 (str - ProcessPool pickling 호환).
        common: 공통 파라미터 dict (start/end/min_is/rebal/top/test_file).

    Returns:
        build_summary_row() 형식의 dict.
    """
    # 워커 내부에서 import (ProcessPool spawn 호환)
    from service.backtest.overfit_diagnostics import generate_overfit_report
    from service.backtest.walk_forward_engine import WalkForwardEngine

    case_dir = Path(out_root) / case["name"]
    case_dir.mkdir(parents=True, exist_ok=True)

    # Python logging 을 case_dir/run.log 로 리디렉트 (stdout/stderr 는 parent 에 inherit).
    # print() 호출이나 numpy/pandas 런타임 warning 은 여전히 parent 로 나오지만,
    # 이 프로젝트는 logging 을 쓰므로 실용상 문제 없음.
    log_path = case_dir / "run.log"
    log_fh = log_path.open("w", encoding="utf-8")

    # logging 레벨 WARNING 으로 올려 rich progress 억제
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    # 기존 핸들러 제거 후 파일 핸들러만 추가
    # 주의: 기존 핸들러를 모두 제거 (Rich progress bar 억제 목적).
    # ProcessPool 워커에서는 격리된 state 이므로 안전. --sequential 모드에서는
    # 메인 프로세스의 logging 설정을 영구적으로 변경하므로 의도적 (디버그용 모드).
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)
    fh = logging.StreamHandler(log_fh)
    fh.setLevel(logging.WARNING)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root_logger.addHandler(fh)

    # tc_annual_rate: transaction_cost_bps / 1e4 * (12 / weight_rebal_months)
    tc_bps = 30.0  # default; overridable via common
    weight_rebal = int(common.get("weight_rebal_months", 3))
    tc_annual_rate = (common.get("transaction_cost_bps", tc_bps) / 1e4) * (12.0 / max(weight_rebal, 1))

    t0 = time.time()
    try:
        engine = WalkForwardEngine(
            min_is_months=common["min_is_months"],
            factor_rebal_months=common["factor_rebal_months"],
            weight_rebal_months=common["weight_rebal_months"],
            top_factors=common["top_factors"],
            turnover_smoothing_alpha=case["alpha"],
            pipeline_params_override=case["override"],
        )
        result = engine.run(
            common["start"], common["end"], test_file=common.get("test_file"),
        )

        # walk_forward CSV
        result.to_csv(str(case_dir / "walk_forward.csv"))

        # overfit 진단
        overfit_report = generate_overfit_report(
            result, full_period_cagr=result.is_full_period_cagr,
        )

        # overfit_diagnostics.csv (기존 main.py 포맷과 동일)
        _save_overfit_diagnostics_csv(overfit_report, case_dir / "overfit_diagnostics.csv")

        # avg_turnover
        avg_to = compute_avg_turnover(result.weight_history)

        runtime = time.time() - t0
        summary = build_summary_row(
            case, overfit_report, avg_to, runtime, "OK", None,
            tc_annual_rate=tc_annual_rate,
        )

        # performance.json (summary 와 동일 필드)
        with (case_dir / "performance.json").open("w", encoding="utf-8") as f:
            json.dump(_json_safe(summary), f, ensure_ascii=False, indent=2)

        return summary
    except Exception as e:
        tb = traceback.format_exc()
        log_fh.write(f"\n\n[FAILED] {type(e).__name__}: {e}\n{tb}\n")
        runtime = time.time() - t0
        return build_summary_row(
            case, None, float("nan"), runtime, "FAILED", f"{type(e).__name__}: {e}",
            tc_annual_rate=tc_annual_rate,
        )
    finally:
        log_fh.flush()
        log_fh.close()


def _json_safe(d: dict[str, Any]) -> dict[str, Any]:
    """NaN/Inf 를 None 으로, numpy 타입을 native 로 변환 (JSON 호환).

    주의: numpy 타입이 Python primitive 의 subclass 라서 분기 순서 중요.
    numpy.float64 은 float 의 subclass 이므로 np.floating 체크를 먼저.
    이 함수는 flat dict 전용 — 중첩 dict 은 재귀하지 않는다.
    """
    out: dict[str, Any] = {}
    for k, v in d.items():
        # numpy types first (float64 is a subclass of float, bool_ is separate)
        if isinstance(v, np.bool_):
            out[k] = bool(v)
        elif isinstance(v, np.integer):
            out[k] = int(v)
        elif isinstance(v, np.floating):
            out[k] = None if np.isnan(v) or np.isinf(v) else float(v)
        elif isinstance(v, float):
            out[k] = None if (math.isnan(v) or math.isinf(v)) else v
        else:
            out[k] = v
    return out


def _save_overfit_diagnostics_csv(report: dict[str, Any], path: Path) -> None:
    """기존 main.py overfit_diagnostics.csv 포맷의 subset 을 저장.

    main.py 의 전체 ~22행 포맷 중 핵심 14행 (Funnel 4 + 지표 4 + 성과 6) 만 출력.
    MDD 서브필드/p-value/주의사항/한계점은 summary 리포트에서 별도 표시되므로 생략.
    전체 포맷이 필요하면 `python main.py backtest` CLI 경로 사용.
    """
    def _pct(v):
        return f"{v:.4%}" if isinstance(v, float) and not np.isnan(v) else "N/A"

    def _dec(v):
        return f"{v:.4f}" if isinstance(v, float) and not np.isnan(v) else "N/A"

    rows = [
        ("1순위 - Funnel Value-Add", "패턴", report["funnel_pattern"], report["funnel_interpretation"]),
        ("1순위 - Funnel Value-Add", "EW_All CAGR", _pct(report["funnel_ew_all_cagr"]), "전체 유효 팩터 동일가중"),
        ("1순위 - Funnel Value-Add", "EW_Top50 CAGR", _pct(report["funnel_ew_top50_cagr"]), "Top-50 후보군 동일가중"),
        ("1순위 - Funnel Value-Add", "Constrained EW CAGR", _pct(report["funnel_cew_cagr"]), "Constrained EW (Top-N + style_cap)"),
        ("2순위 - OOS Percentile", "평균 백분위", _pct(report["oos_avg_percentile"]), report["oos_percentile_interpretation"]),
        ("3순위 - Strict Jaccard", "Strict Jaccard", _dec(report["strict_jaccard"]), report["strict_jaccard_interpretation"]),
        ("4순위(보조) - Rank Corr", "IS-OOS Rank Correlation", _dec(report["is_oos_rank_spearman"]), report["rank_corr_interpretation"]),
        ("5순위(보조) - Deflation", "Deflation Ratio", _dec(report["deflation_ratio"]), report["deflation_interpretation"]),
        ("OOS 성과 - Constrained EW", "CAGR", _pct(report["oos_cagr"]), ""),
        ("OOS 성과 - Constrained EW", "MDD", _pct(report["oos_mdd"]), ""),
        ("OOS 성과 - Constrained EW", "Sharpe", _dec(report["oos_sharpe"]), ""),
        ("OOS 성과 - Constrained EW", "Calmar", _dec(report["oos_calmar"]), ""),
        ("OOS 성과 - EW", "CAGR", _pct(report["oos_ew_cagr"]), ""),
        ("OOS 성과 - EW", "Sharpe", _dec(report["oos_ew_sharpe"]), ""),
    ]
    pd.DataFrame(rows, columns=["Category", "Metric", "Value", "Interpretation"]).to_csv(
        path, index=False, encoding="utf-8-sig",
    )


def pick_recommendation(summary_df: pd.DataFrame) -> dict[str, Any] | None:
    """summary_df 에서 추천 케이스 1개를 선정.

    규칙: status==OK AND verdict==OK 인 행 중, sharpe_cew 상위 3개를 추린 뒤
    그 중 avg_turnover 가 가장 낮은 행을 선택.

    Args:
        summary_df: build_summary_row 의 행들로 구성된 DataFrame.

    Returns:
        선정된 행의 dict, 또는 후보 없음 시 None.
    """
    ok_rows = summary_df[
        (summary_df["status"] == "OK") & (summary_df["verdict"] == "OK")
    ].copy()
    if len(ok_rows) == 0:
        return None
    top3 = ok_rows.nlargest(min(3, len(ok_rows)), "sharpe_cew")
    # avg_turnover 가 모두 NaN 인 경우 (OOS <2 rebalance) idxmin 이 NaN 반환 -> KeyError 방지
    with_turnover = top3.dropna(subset=["avg_turnover"])
    if len(with_turnover) == 0:
        return top3.iloc[0].to_dict()
    best = with_turnover.loc[with_turnover["avg_turnover"].idxmin()]
    return best.to_dict()


def render_markdown_report(
    summary_df: pd.DataFrame,
    out_path: Path,
    meta: dict[str, Any],
) -> None:
    """summary_df 를 입력받아 REPORT.md 생성.

    spec §5 포맷 - §1 성과 요약 / §2 과적합 진단 / §3 해석 / §4 추천 / §5 메타.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # baseline 기준선
    baseline = summary_df[summary_df["case"] == "baseline"]
    bl_cagr = float(baseline["cagr_cew"].iloc[0]) if len(baseline) else float("nan")
    bl_turnover = float(baseline["avg_turnover"].iloc[0]) if len(baseline) else float("nan")

    lines: list[str] = []
    lines.append("# Cluster Dedup x Turnover Smoothing 실험 리포트")
    lines.append("")
    lines.append(f"- 실행일: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- Git SHA: `{meta.get('git_sha', 'unknown')}`")
    lines.append(f"- 백테스트 기간: `{meta.get('start', '?')}` ~ `{meta.get('end', '?')}`")
    lines.append(
        f"- 공통 파라미터: min_is={meta.get('min_is_months', 36)}, "
        f"factor_rebal={meta.get('factor_rebal_months', 6)}, "
        f"weight_rebal={meta.get('weight_rebal_months', 3)}, "
        f"top={meta.get('top_factors', 50)}, ranking=tstat"
    )
    lines.append("")

    # §1 성과 요약
    lines.append("## 1. 성과 요약 (OOS, Net-of-cost)")
    lines.append("")
    lines.append("| 케이스 | CAGR | Net CAGR | Sharpe | MDD | Calmar | Avg Turnover | dCAGR vs base | dTurnover vs base |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for _, r in summary_df.iterrows():
        if r["status"] != "OK":
            lines.append(f"| `{r['case']}` | FAILED: {r.get('error', '')} | | | | | | | |")
            continue
        d_cagr = r["cagr_cew"] - bl_cagr if not np.isnan(bl_cagr) else float("nan")
        d_to = r["avg_turnover"] - bl_turnover if not np.isnan(bl_turnover) else float("nan")
        lines.append(
            f"| `{r['case']}` | {_fmt_pct(r['cagr_cew'])} | {_fmt_pct(r.get('net_cagr_cew'))} | "
            f"{_fmt_dec(r['sharpe_cew'])} | {_fmt_pct(r['mdd_cew'])} | {_fmt_dec(r['calmar_cew'])} | "
            f"{_fmt_dec(r['avg_turnover'])} | "
            f"{_fmt_pct_signed(d_cagr)} | {_fmt_dec_signed(d_to)} |"
        )
    lines.append("")

    # §2 과적합 진단
    lines.append("## 2. 과적합 진단")
    lines.append("")
    lines.append("| 케이스 | Verdict | Funnel (A/B/C CAGR) | OOS Pctile (lower=better) | Jaccard (higher=better) | Rank Corr (higher=better) | Deflation |")
    lines.append("|---|---|---|---|---|---|---|")
    for _, r in summary_df.iterrows():
        if r["status"] != "OK":
            lines.append(f"| `{r['case']}` | FAILED | | | | | |")
            continue
        funnel_str = (
            f"{r['funnel_verdict']} ({_fmt_pct(r['funnel_a_cagr'])}/"
            f"{_fmt_pct(r['funnel_b_cagr'])}/{_fmt_pct(r['funnel_c_cagr'])})"
        )
        pctile_str = f"{_fmt_pct(r['oos_pctile_value'])} [{r['oos_pctile_flag']}]"
        lines.append(
            f"| `{r['case']}` | **{r['verdict']}** | {funnel_str} | {pctile_str} | "
            f"{_fmt_dec(r['strict_jaccard'])} | {_fmt_dec(r['is_oos_rank_corr'])} | "
            f"{_fmt_dec(r['deflation_ratio'])} |"
        )
    lines.append("")
    lines.append("> *Deflation Ratio = OOS CAGR / IS CAGR. OOS 기간이 짧으면 단독 판단 금지.*")
    lines.append("")

    # §3 해석 (자동 스켈레톤)
    lines.append("## 3. 해석")
    lines.append("")
    lines.extend(_render_interpretation(summary_df, bl_cagr, bl_turnover))
    lines.append("")

    # §4 추천 조합
    lines.append("## 4. 추천 조합")
    lines.append("")
    best = pick_recommendation(summary_df)
    if best is None:
        lines.append("- 추천 가능한 케이스 없음 (모두 FAILED 또는 과적합 판정)")
    else:
        lines.append(
            f"- 선정 규칙: `verdict==OK` 중 Sharpe 상위 3개, 그 중 `avg_turnover` 최저"
        )
        lines.append(f"- **최종 추천: `{best['case']}`**")
        lines.append(
            f"  - 근거: CAGR {_fmt_pct(best['cagr_cew'])}, "
            f"Sharpe {_fmt_dec(best['sharpe_cew'])}, "
            f"Avg Turnover {_fmt_dec(best['avg_turnover'])} (baseline 대비 "
            f"dCAGR {_fmt_pct_signed(best['cagr_cew'] - bl_cagr)}, "
            f"dTurnover {_fmt_dec_signed(best['avg_turnover'] - bl_turnover)})"
        )
    lines.append("")

    # §5 실행 메타
    lines.append("## 5. 실행 메타")
    lines.append("")
    lines.append(f"- 워커 수: {meta.get('workers', '?')}")
    total_runtime = summary_df["runtime_sec"].fillna(0).sum()
    lines.append(f"- 총 소요 시간 (순차 합): {total_runtime:.1f}s")
    lines.append("")
    lines.append("| 케이스 | 상태 | Runtime (s) |")
    lines.append("|---|---|---|")
    for _, r in summary_df.iterrows():
        lines.append(f"| `{r['case']}` | {r['status']} | {r['runtime_sec']:.1f} |")
    lines.append("")
    failed = summary_df[summary_df["status"] == "FAILED"]
    if len(failed):
        lines.append("### 실패 케이스")
        for _, r in failed.iterrows():
            lines.append(f"- `{r['case']}`: {r.get('error', 'Unknown error')}")
        lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")


def _render_interpretation(
    df: pd.DataFrame, bl_cagr: float, bl_turnover: float,
) -> list[str]:
    """§3 해석 섹션의 자동 스켈레톤 (방향성/수치만; 도메인 해석은 사람 보강).

    주의: build_cases() 의 케이스 이름 (baseline, cluster_12/18/24,
    smooth_0.7/0.5, combo_18_0.7/0.5) 에 하드코딩 되어 있다. 케이스 이름을
    바꾸려면 이 함수도 동기화해야 한다 (실패 시 §3 하위 섹션이 조용히 비어 나옴).
    """
    def _row(name: str) -> dict | None:
        sub = df[df["case"] == name]
        return sub.iloc[0].to_dict() if len(sub) else None

    lines: list[str] = []

    # 3.1 Clustering 단독 (baseline vs cluster_18)
    lines.append("### 3.1 Clustering 효과 (baseline vs cluster_18)")
    r2 = _row("cluster_18")
    bl_row = _row("baseline")
    if r2 and r2["status"] == "OK" and bl_row and bl_row["status"] == "OK":
        lines.append(
            f"- dCAGR: {_fmt_pct_signed(r2['cagr_cew'] - bl_cagr)}, "
            f"dSharpe: {_fmt_dec_signed(r2['sharpe_cew'] - bl_row['sharpe_cew'])}, "
            f"dTurnover: {_fmt_dec_signed(r2['avg_turnover'] - bl_turnover)}"
        )
        lines.append(f"- Verdict: baseline=`{bl_row['verdict']}`, cluster_18=`{r2['verdict']}`")
    lines.append("")

    # 3.2 n_clusters 민감도
    lines.append("### 3.2 n_clusters 민감도 (cluster_12 / cluster_18 / cluster_24)")
    for name in ["cluster_12", "cluster_18", "cluster_24"]:
        r = _row(name)
        if r and r["status"] == "OK":
            lines.append(
                f"- `{name}`: CAGR {_fmt_pct(r['cagr_cew'])}, "
                f"Sharpe {_fmt_dec(r['sharpe_cew'])}, "
                f"Turnover {_fmt_dec(r['avg_turnover'])}, Verdict `{r['verdict']}`"
            )
    lines.append("")

    # 3.3 Smoothing 단독
    lines.append("### 3.3 Turnover Smoothing 단독 효과 (baseline / smooth_0.7 / smooth_0.5)")
    for name in ["baseline", "smooth_0.7", "smooth_0.5"]:
        r = _row(name)
        if r and r["status"] == "OK":
            lines.append(
                f"- `{name}` (alpha={r['turnover_alpha']}): "
                f"CAGR {_fmt_pct(r['cagr_cew'])}, Turnover {_fmt_dec(r['avg_turnover'])}"
            )
    lines.append("")

    # 3.4 조합
    lines.append("### 3.4 조합 효과 (cluster_18 / combo_18_0.7 / combo_18_0.5)")
    for name in ["cluster_18", "combo_18_0.7", "combo_18_0.5"]:
        r = _row(name)
        if r and r["status"] == "OK":
            lines.append(
                f"- `{name}` (alpha={r['turnover_alpha']}): "
                f"CAGR {_fmt_pct(r['cagr_cew'])}, Sharpe {_fmt_dec(r['sharpe_cew'])}, "
                f"Turnover {_fmt_dec(r['avg_turnover'])}"
            )
    lines.append("")
    lines.append("> *§3 자동 해석은 방향성/수치만 제시. 도메인 해석은 사람이 보강.*")
    return lines


def _fmt_pct(v: Any) -> str:
    try:
        if v is None or (isinstance(v, float) and v != v):
            return "N/A"
        return f"{float(v):.2%}"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_dec(v: Any) -> str:
    try:
        if v is None or (isinstance(v, float) and v != v):
            return "N/A"
        return f"{float(v):.3f}"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_pct_signed(v: Any) -> str:
    try:
        if v is None or (isinstance(v, float) and v != v):
            return "N/A"
        return f"{float(v):+.2%}"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_dec_signed(v: Any) -> str:
    try:
        if v is None or (isinstance(v, float) and v != v):
            return "N/A"
        return f"{float(v):+.3f}"
    except (TypeError, ValueError):
        return "N/A"


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT,
        ).decode().strip()
    except Exception:
        return "unknown"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Hierarchical Clustering x Turnover Smoothing 실험 러너",
    )
    p.add_argument("--start", default="2009-12-31", help="백테스트 시작일 (기본: 2009-12-31)")
    p.add_argument("--end", default="2026-03-31", help="백테스트 종료일 (기본: 2026-03-31)")
    p.add_argument("--min-is-months", type=int, default=36)
    p.add_argument("--factor-rebal-months", type=int, default=6)
    p.add_argument("--weight-rebal-months", type=int, default=3)
    p.add_argument("--top-factors", type=int, default=50)
    p.add_argument("--workers", type=int, default=4, help="병렬 워커 수 (기본 4)")
    p.add_argument("--sequential", action="store_true", help="순차 실행 (디버그용)")
    p.add_argument("--test-mode", type=str, default=None,
                   help="test_data.csv 파일명 (소량 smoke test 용)")
    p.add_argument("--only", type=str, default=None,
                   help="쉼표 구분 케이스 이름만 실행 (예: baseline,cluster_18)")
    p.add_argument("--out-root", type=str, default=None,
                   help="결과 저장 루트 (기본: output/experiments/cluster_turnover_<ts>)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv)

    cases = build_cases()
    if args.only:
        wanted = {s.strip() for s in args.only.split(",")}
        cases = [c for c in cases if c["name"] in wanted]
        if not cases:
            logger.error("--only 가 어떤 케이스도 매칭하지 않음: %s", args.only)
            return 2

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = Path(args.out_root) if args.out_root else (
        ROOT / "output" / "experiments" / f"cluster_turnover_{ts}"
    )
    out_root.mkdir(parents=True, exist_ok=True)

    common = {
        "start": args.start,
        "end": args.end,
        "min_is_months": args.min_is_months,
        "factor_rebal_months": args.factor_rebal_months,
        "weight_rebal_months": args.weight_rebal_months,
        "top_factors": args.top_factors,
        "test_file": args.test_mode,
    }

    # config.json
    config = {
        "run_id": out_root.name,
        "git_sha": _git_sha(),
        "backtest_start": args.start,
        "backtest_end": args.end,
        "min_is_months": args.min_is_months,
        "factor_rebal_months": args.factor_rebal_months,
        "weight_rebal_months": args.weight_rebal_months,
        "top_factors": args.top_factors,
        "factor_ranking_method": "tstat",
        "workers": 1 if args.sequential else args.workers,
        "test_mode": bool(args.test_mode),
        "cases": cases,
    }
    with (out_root / "config.json").open("w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    logger.info("Experiment started: %s (%d cases)", out_root, len(cases))

    rows: list[dict[str, Any]] = []
    if args.sequential or args.workers == 1 or args.test_mode:
        # 순차 실행 (test_mode 도 순차로 강제 - 메모리 절약)
        for i, c in enumerate(cases, 1):
            logger.info("[%d/%d] running %s", i, len(cases), c["name"])
            row = run_single_case(c, str(out_root), common)
            logger.info("[%d/%d] %s -> %s (%.1fs)", i, len(cases), c["name"],
                        row["status"], row["runtime_sec"])
            rows.append(row)
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futures = {
                ex.submit(run_single_case, c, str(out_root), common): c for c in cases
            }
            done = 0
            for fut in as_completed(futures):
                case = futures[fut]
                try:
                    row = fut.result()
                except Exception as e:
                    row = build_summary_row(
                        case, None, float("nan"), 0.0,
                        "FAILED", f"{type(e).__name__}: {e}",
                    )
                done += 1
                logger.info("[%d/%d] %s -> %s (%.1fs)",
                            done, len(cases), case["name"],
                            row["status"], row["runtime_sec"])
                rows.append(row)

    # 케이스 이름 순서로 정렬 (build_cases 정의 순서)
    name_order = {c["name"]: i for i, c in enumerate(cases)}
    rows.sort(key=lambda r: name_order.get(r["case"], 999))

    summary_df = pd.DataFrame(rows)
    summary_df.to_csv(out_root / "summary.csv", index=False)

    render_markdown_report(
        summary_df,
        out_root / "REPORT.md",
        meta={
            "git_sha": config["git_sha"],
            "start": args.start,
            "end": args.end,
            "min_is_months": args.min_is_months,
            "factor_rebal_months": args.factor_rebal_months,
            "weight_rebal_months": args.weight_rebal_months,
            "top_factors": args.top_factors,
            "workers": config["workers"],
        },
    )

    n_ok = (summary_df["status"] == "OK").sum()
    n_fail = (summary_df["status"] == "FAILED").sum()
    logger.info(
        "Experiment complete: %d OK / %d FAILED -> %s",
        n_ok, n_fail, out_root / "REPORT.md",
    )
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
