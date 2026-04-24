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
