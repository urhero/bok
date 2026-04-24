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
    """실험 8 케이스 정의.

    per_cluster_keep=3 고정, 한 축씩 변화시켜 효과를 분리한다.

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

    # 워커 stdout/stderr 를 case_dir/run.log 로 리디렉트
    log_path = case_dir / "run.log"
    log_fh = log_path.open("w", encoding="utf-8")

    # logging 레벨 WARNING 으로 올려 rich progress 억제
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    # 기존 핸들러 제거 후 파일 핸들러만 추가
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
    """NaN/Inf 를 None 으로, numpy 타입을 native 로 변환 (JSON 호환)."""
    out: dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, float) and (v != v or v in (float("inf"), float("-inf"))):
            out[k] = None
        elif isinstance(v, (np.integer,)):
            out[k] = int(v)
        elif isinstance(v, (np.floating,)):
            out[k] = None if np.isnan(v) else float(v)
        elif isinstance(v, (np.bool_,)):
            out[k] = bool(v)
        else:
            out[k] = v
    return out


def _save_overfit_diagnostics_csv(report: dict[str, Any], path: Path) -> None:
    """기존 main.py 의 overfit_diagnostics.csv 포맷을 재현."""
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
