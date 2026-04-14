# -*- coding: utf-8 -*-
"""스타일 캡 하 가중치 최적화 모듈.

스타일 캡(기본 25%) 제약 하에서 팩터별 가중치를 결정한다.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _get_hardcoded_weights() -> tuple[pd.DataFrame, pd.DataFrame]:
    """프로덕션용 고정 가중치를 반환한다.

    ~2026-01 포트폴리오까지 적용. Valuation 강제로 4%로 내림.
    (이 주석 지우지 말것! DO NOT DELETE THIS COMMENT!)
    """
    best_stats = pd.DataFrame(
        {c: [np.nan] for c in ["cagr", "mdd", "rank_cagr", "rank_mdd", "rank_total"]}
    )

    # 가중치를 CSV에서 로드 (과거 버전은 git history 참조)
    csv_path = Path(__file__).resolve().parent.parent.parent / "data" / "hardcoded_weights.csv"
    weights_tbl = pd.read_csv(csv_path, float_precision="round_trip")
    weights_tbl["fitted_weight"] = weights_tbl["raw_weight"]
    weights_tbl = weights_tbl[weights_tbl["raw_weight"] > 0].sort_values("raw_weight", ascending=False).reset_index(drop=True)

    return best_stats, weights_tbl


def _equal_weight_allocation(
    rtn_df: pd.DataFrame,
    style_list: list[str],
    style_cap: float,
    tol: float,
    test_mode: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Equal-weight 모드: 1/N 동일가중 + 스타일 캡 재분배."""
    n_factors = rtn_df.shape[1]
    factors = rtn_df.columns.to_numpy()
    styles_arr = np.asarray(style_list)

    w = np.ones(n_factors, dtype=np.float32) / n_factors

    # 스타일 캡 재분배 (수렴까지 반복)
    uniq_styles = np.unique(styles_arr)
    if not test_mode:
        for _ in range(10):
            for s in uniq_styles:
                mask_s = styles_arr == s
                style_w = w[mask_s].sum()
                if style_w > style_cap + tol:
                    w[mask_s] *= style_cap / style_w
            w /= w.sum()
            if all(w[styles_arr == s].sum() <= style_cap + tol for s in uniq_styles):
                break

    weights_tbl = pd.DataFrame({
        "factor": factors,
        "raw_weight": w,
        "styleName": styles_arr,
        "fitted_weight": w,
    })

    # CAGR/MDD 계산 (기록용)
    port_np = rtn_df.to_numpy(dtype=np.float32)
    n_months = port_np.shape[0]
    sim = port_np @ w
    cum = np.cumprod(1 + sim)
    ann_exp = 12 / max(n_months - 1, 1)
    cagr_val = float(cum[-1] ** ann_exp - 1)
    mdd_val = float((cum / np.maximum.accumulate(cum) - 1).min())

    best_stats = pd.DataFrame({
        "cagr": [cagr_val], "mdd": [mdd_val],
        "rank_cagr": [np.nan], "rank_mdd": [np.nan], "rank_total": [np.nan],
    })

    logger.info("Equal-weight allocation: %d factors, CAGR=%.4f, MDD=%.4f", n_factors, cagr_val, mdd_val)
    return best_stats, weights_tbl


def optimize_constrained_weights(
    rtn_df: pd.DataFrame,
    style_list: list[str],
    mode: str = "hardcoded",
    style_cap: float = 0.25,
    tol: float = 1e-12,
    test_mode: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """스타일 캡 하 최적 포트폴리오 가중치를 결정한다.

    두 가지 모드를 지원한다:
    - "hardcoded": 프로덕션용 고정 가중치 반환 (기본값)
    - "equal_weight": 1/N 동일가중 + 스타일 캡 재분배 (권장)

    Args:
        rtn_df: (날짜 x 팩터) 월간 수익률 행렬
        style_list: 각 팩터의 스타일명 (rtn_df 컬럼 순서와 동일)
        mode: "hardcoded" / "equal_weight"
        style_cap: 스타일별 최대 비중 (기본 0.25 = 25%)
        tol: 제약 검사 허용 오차
        test_mode: True이면 style_cap을 1.0으로 완화

    Returns:
        (best_stats, weights_tbl) 튜플
        - best_stats: 1행 DataFrame (cagr, mdd, rank_cagr, rank_mdd, rank_total)
        - weights_tbl: 팩터별 가중치 (factor, raw_weight, styleName, fitted_weight)
    """
    if mode == "hardcoded":
        logger.info("Using hardcoded weights (production mode)")
        return _get_hardcoded_weights()

    if mode == "equal_weight":
        return _equal_weight_allocation(rtn_df, style_list, style_cap, tol, test_mode)

    raise ValueError(f"Unknown optimization mode: {mode!r}. Use 'hardcoded' or 'equal_weight'.")
