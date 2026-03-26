# -*- coding: utf-8 -*-
"""포트폴리오 블렌딩 모듈.

Core 파이프라인(기존 CAGR 랭킹 또는 Factor Timing)과
Satellite(TFT Stock Prediction)의 비중을 합산하여 최종 MP를 구성한다.
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def normalize_to_unit_exposure(weights: pd.Series) -> pd.Series:
    """Long 합 = +1.0, Short 합 = -1.0이 되도록 각각 스케일링.

    Args:
        weights: 종목별 비중 시리즈

    Returns:
        정규화된 비중 시리즈
    """
    w = weights.copy()
    long_sum = w[w > 0].sum()
    short_sum = w[w < 0].abs().sum()

    if long_sum > 0:
        w.loc[w > 0] = w[w > 0] / long_sum
    if short_sum > 0:
        w.loc[w < 0] = w[w < 0] / short_sum

    return w


def blend_portfolios(
    core_weights_df: pd.DataFrame,
    tft_weights_df: pd.DataFrame,
    core_ratio: float = 0.80,
    tft_ratio: float = 0.20,
) -> pd.DataFrame:
    """두 MP를 가중합산.

    Args:
        core_weights_df: DataFrame with (gvkeyiid, core_weight)
        tft_weights_df: DataFrame with (gvkeyiid, tft_weight)
        core_ratio: Core 비중 (default 0.80)
        tft_ratio: TFT Satellite 비중 (default 0.20)

    Returns:
        DataFrame(gvkeyiid, core_weight, tft_weight, blended_weight)
        - 양쪽에만 있는 종목도 처리 (outer join, 결측=0)
        - 합산 전 양쪽 모두 Long합=+1, Short합=-1로 정규화
        - 합산 후에도 재정규화
    """
    # 입력 컬럼명 정리
    core = core_weights_df.copy()
    tft = tft_weights_df.copy()

    # core_weight 컬럼 확인
    if "core_weight" not in core.columns:
        # stock_weight, weight 등 다른 이름일 수 있음
        weight_candidates = ["stock_weight", "weight", "mp_ls_weight"]
        for c in weight_candidates:
            if c in core.columns:
                core = core.rename(columns={c: "core_weight"})
                break
        if "core_weight" not in core.columns:
            # 마지막 숫자 컬럼 사용
            num_cols = core.select_dtypes(include=[np.number]).columns
            if len(num_cols) > 0:
                core = core.rename(columns={num_cols[-1]: "core_weight"})
            else:
                logger.warning("[blend] core_weight 컬럼 없음 → 빈 결과")
                core["core_weight"] = 0.0

    if "tft_weight" not in tft.columns:
        weight_candidates = ["stock_weight", "weight", "predicted_weight"]
        for c in weight_candidates:
            if c in tft.columns:
                tft = tft.rename(columns={c: "tft_weight"})
                break
        if "tft_weight" not in tft.columns:
            num_cols = tft.select_dtypes(include=[np.number]).columns
            if len(num_cols) > 0:
                tft = tft.rename(columns={num_cols[-1]: "tft_weight"})
            else:
                tft["tft_weight"] = 0.0

    # gvkeyiid 기준 outer join
    merge_keys = ["gvkeyiid"]
    # ticker, isin이 있으면 추가 키로 사용
    for extra in ["ticker", "isin"]:
        if extra in core.columns and extra in tft.columns:
            merge_keys.append(extra)
        elif extra in core.columns:
            # tft에 없으면 제거 방지 위해 별도 처리
            pass

    # 필수 컬럼만 추출
    core_cols = merge_keys + ["core_weight"]
    tft_cols = ["gvkeyiid", "tft_weight"]

    core_sub = core[[c for c in core_cols if c in core.columns]].copy()
    tft_sub = tft[[c for c in tft_cols if c in tft.columns]].copy()

    # 종목별 합산 (동일 gvkeyiid가 여러 행일 수 있음)
    core_sub = core_sub.groupby("gvkeyiid", as_index=False)["core_weight"].sum()
    tft_sub = tft_sub.groupby("gvkeyiid", as_index=False)["tft_weight"].sum()

    merged = core_sub.merge(tft_sub, on="gvkeyiid", how="outer")
    merged["core_weight"] = merged["core_weight"].fillna(0.0)
    merged["tft_weight"] = merged["tft_weight"].fillna(0.0)

    # 1) 정규화: Long합=+1, Short합=-1
    merged["core_weight_scaled"] = normalize_to_unit_exposure(merged["core_weight"])
    merged["tft_weight_scaled"] = normalize_to_unit_exposure(merged["tft_weight"])

    # 2) 가중합산
    merged["blended_weight"] = (
        core_ratio * merged["core_weight_scaled"]
        + tft_ratio * merged["tft_weight_scaled"]
    )

    # 3) 합산 후 재정규화
    merged["blended_weight"] = normalize_to_unit_exposure(merged["blended_weight"])

    # 원래 비중도 유지 (디버깅용)
    result = merged[["gvkeyiid", "core_weight", "tft_weight", "blended_weight"]].copy()

    # ticker, isin 정보 복원 (있으면)
    if "ticker" in core.columns:
        ticker_map = core.drop_duplicates("gvkeyiid")[["gvkeyiid", "ticker"]]
        result = result.merge(ticker_map, on="gvkeyiid", how="left")
    if "isin" in core.columns:
        isin_map = core.drop_duplicates("gvkeyiid")[["gvkeyiid", "isin"]]
        result = result.merge(isin_map, on="gvkeyiid", how="left")

    logger.info(
        "[blend] Core %.0f%% + TFT %.0f%% → %d 종목 (Long=%d, Short=%d, Neutral=%d)",
        core_ratio * 100,
        tft_ratio * 100,
        len(result),
        (result["blended_weight"] > 1e-8).sum(),
        (result["blended_weight"] < -1e-8).sum(),
        ((result["blended_weight"].abs()) <= 1e-8).sum(),
    )

    return result
