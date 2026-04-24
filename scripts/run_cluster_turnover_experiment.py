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
