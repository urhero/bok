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
) -> dict[str, Any]:
    """케이스 1개의 결과를 summary.csv 한 행 dict 로 변환.

    `overfit_report` 가 None 이면 FAILED 케이스.
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
        for k in [
            "cagr_cew", "sharpe_cew", "mdd_cew", "calmar_cew",
            "cagr_ew", "sharpe_ew",
            "funnel_a_cagr", "funnel_b_cagr", "funnel_c_cagr",
            "oos_pctile_value", "strict_jaccard", "is_oos_rank_corr", "deflation_ratio",
        ]:
            row[k] = float("nan")
        row["funnel_verdict"] = "N/A"
        row["oos_pctile_flag"] = "N/A"
        row["verdict"] = "N/A"
        return row

    pattern = overfit_report["funnel_pattern"]
    pctile = overfit_report.get("oos_avg_percentile", float("nan"))

    row.update({
        "cagr_cew": overfit_report["oos_cagr"],
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
