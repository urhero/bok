# -*- coding: utf-8 -*-
"""
엔드투엔드 팩터 파이프라인 (v4-complete)
=======================================
이 파이프라인은 원시 팩터 데이터를 정제하고, 월간 스프레드 수익률 행렬을 구성하며,
CAGR로 팩터 순위를 매기고, 2-팩터 믹스를 최적화하며, CSV 결과물을 내보냅니다.

**`meta['factorAbbreviation']`에 지정된 컬럼 순서를 유지합니다.**

주요 출력물
-----------
| File                          | Description                                                      |
|-------------------------------|------------------------------------------------------------------|
| `final_pivot_yymmdd.csv`      | 개별 팩터 노출도가 포함된 피벗된 가중치 행렬            |
| `final_style_yymmdd.csv`      | 스타일별로 분류된 팩터 가중치 패널 데이터                   |
| `final_factor_yymmdd.csv`     | 개별 팩터 노출도가 포함된 가중치 패널 데이터                |
| `final_mp_yymmdd.csv`         | 실행 가능한 모델 포트폴리오                                       |
"""
from __future__ import annotations

import logging
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import numpy as np
import pandas as pd
from rich.progress import track

from config import PARAM

# ---------------------------------------------------------------------------
# 로깅 설정
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# =============================================================================
# 수치 계산 헬퍼 유틸리티
# =============================================================================
def prepend_start_zero(series: pd.DataFrame) -> pd.DataFrame:
    """첫 관측값 한 달 전에 0을 삽입 (기준선 설정)"""
    series.loc[series.index[0] - pd.DateOffset(months=1)] = 0
    return series.sort_index()


# ----------------------------------------------------------------------------
# 핵심 팩터 할당 로직
# ----------------------------------------------------------------------------

def calculate_factor_stats(
    factor_abbr: str,
    sort_order: int,
    factor_data_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame] | Tuple[None, None, None, None]:
    """특정 팩터에 대한 섹터/분위수/스프레드 수익률 계산

    Parameters
    ----------
    factor_abbr : str
        팩터 약어
    sort_order : int
        순위 방향 (1=오름차순, 0/-1=내림차순)
    factor_data_df : pd.DataFrame
        해당 팩터의 데이터프레임 (이미 필터링됨, M_RETURN 포함)

    Returns
    -------
    Tuple[DataFrame, DataFrame, DataFrame, DataFrame] | tuple[None, None, None, None]
        ``sector_ret``   – 섹터 × 분위수(Q1‑Q5)별 평균 월간 수익률
        ``quantile_ret`` – 분위수(Q1‑Q5)별 전체 시장 수익률
        ``spread``       – 롱‑숏(Q1‑Q5) 월간 성과 시계열
        ``merged``       – 계산에 사용된 기본 종목 수준 데이터프레임

        팩터의 데이터 포인트가 ≤100개인 경우, 4개 요소 모두 ``None``
    """
    logger.debug(f"[Trace] Processing factor {factor_abbr}. Data shape: {factor_data_df.shape}")

    # ------------------------------------------------------------------
    # 1. 팩터 시계열 수집 및 래그 적용
    # ------------------------------------------------------------------
    # factor_data_df는 이미 필터링되어 전달됨
    factor_data_df = factor_data_df.dropna().reset_index(drop=True)

    # 히스토리가 충분하지 않으면 스킵
    if len(factor_data_df['ddt'].unique()) <= 2:
        logger.warning("Skipping %s – insufficient history", factor_abbr)
        return None, None, None, None

    # 각 종목별로 1개월 래그 적용 (전월 팩터 값을 당월에 사용)
    # val은 이미 존재, M_RETURN도 이미 존재
    factor_data_df[factor_abbr] = factor_data_df.groupby("gvkeyiid")["val"].shift(1)

    # 팩터 래그 생성 후 NaN 제거 + 필요한 컬럼 정리
    # M_RETURN과 필수 Key들은 이미 존재함
    merged_df = (
        factor_data_df
        .dropna(subset=[factor_abbr, "M_RETURN"])  # 팩터도 있고 수익률도 있는 구간만
        .drop(columns=["val", "factorAbbreviation"])
        .reset_index(drop=True)
    )

    # ------------------------------------------------------------------
    # 4. 섹터 내 순위 매기기, 점수화, 분위수 버킷 할당
    # ------------------------------------------------------------------
    # 날짜 및 섹터별로 팩터 값에 대한 순위 계산
    merged_df["rank"] = (
        merged_df.groupby(["ddt", "sec"])[factor_abbr].rank(method="average", ascending=bool(sort_order))
    )
    # 날짜 및 섹터별 데이터 개수 계산 (vectorized)
    count_series = merged_df.groupby(["ddt", "sec"])[factor_abbr].transform("count")

    # Vectorized Percentile: (Rank - 1) / (Count - 1) * 100
    # 데이터 개수가 10개 이하는 NaN 처리 (기존 로직 유지)
    merged_df["percentile"] = (merged_df["rank"] - 1) / (count_series - 1) * 100
    merged_df.loc[count_series <= 10, "percentile"] = np.nan

    # Vectorized Quantile: pd.cut 사용 (apply 제거)
    # 0~20: Q1, 20~40: Q2, ..., 80~100: Q5
    # 기존 logic(math.ceil)과 일치: x=20 -> Q1 / x=20.001 -> Q2.
    # 따라서 (0, 20], (20, 40] ... 구간이 맞음 -> right=True
    # 0 포함을 위해 include_lowest=True 사용
    labels = ["Q1", "Q2", "Q3", "Q4", "Q5"]
    merged_df["quantile"] = pd.cut(
        merged_df["percentile"],
        bins=[0, 20, 40, 60, 80, 105],  # 100 포함
        labels=labels,
        include_lowest=True,
        right=True
    )
    merged_df = merged_df.dropna(subset=["quantile"])

    # 불필요한 중간 컬럼 제거 (Optimization)
    merged_df = merged_df.drop(columns=["rank", "percentile"])

    # ------------------------------------------------------------------
    # 5. 섹터 및 시장 분위수 수익률 계산
    # ------------------------------------------------------------------
    # 섹터별 분위수 평균 수익률 계산 (같은 날짜별 평균 수익률은 산술평균 수익률)
    sector_return_df = (
        merged_df.groupby(["ddt", "sec", "quantile"], observed=False)["M_RETURN"].mean().unstack(fill_value=0)
    ).groupby("sec").mean().T

    # 전체 시장의 분위수별 평균 수익률 계산 (같은 날짜별 평균 수익률은 산술평균 수익률)
    quantile_return_df = merged_df.groupby(["ddt", "quantile"], observed=False)["M_RETURN"].mean().unstack(fill_value=0)

    # ------------------------------------------------------------------
    # 6. Q1‑Q5 스프레드 계산 (롱‑숏 전략)
    # ------------------------------------------------------------------
    # Q1(최고) - Q5(최저) 수익률 차이 계산
    spread_series = pd.DataFrame({factor_abbr: quantile_return_df.iloc[:, 0] - quantile_return_df.iloc[:, -1]})
    spread_series = prepend_start_zero(spread_series)

    logger.debug(f"[Trace] Factor {factor_abbr} assigned. Sector Ret Shape: {sector_return_df.shape}, Quantile Ret Shape: {quantile_return_df.shape}")
    return sector_return_df, quantile_return_df, spread_series, merged_df


# ---------------------------------------------------------------------------
# 전역 경로
# ---------------------------------------------------------------------------
DATA_DIR = Path.cwd() / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = Path.cwd() / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# 2️⃣ 섹터 필터링 + 재라벨링
# =============================================================================
def filter_and_label_factors(
    factor_abbr_list: List[str],
    factor_name_list: List[str],
    style_name_list: List[str],
    factor_data_list: List[Tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None]],
) -> Tuple[List[str], List[str], List[str], List[int], List[List[str]], List[pd.DataFrame]]:
    """음의 Q-스프레드를 가진 섹터 제거; 롱/숏 라벨 재계산"""

    kept_factor_abbrs, kept_name, kept_style, kept_idx = [], [], [], []
    dropped_sec: List[List[str]] = []
    filtered_raw_data_list: List[pd.DataFrame] = []

    for idx, (sector_return_df, _, _, raw_df) in track(
        enumerate(factor_data_list), description="Filtering sectors", total=len(factor_data_list)
    ):
        if sector_return_df is None or raw_df is None:
            logger.debug("Factor %d skipped – no data", idx)
            continue
        tmp = sector_return_df.T.reset_index()
        tmp["spread"] = tmp["Q1"] - tmp["Q5"]  # Q1-Q5 스프레드 계산
        to_drop = tmp.loc[tmp["spread"] < 0, "sec"].tolist()  # 음의 스프레드 섹터 식별
        raw_clean = raw_df[~raw_df["sec"].isin(to_drop)].reset_index(drop=True)  # 해당 섹터 제거
        if raw_clean.empty:
            logger.debug("Factor %d discarded – all sectors dropped", idx)
            continue

        q_ret = raw_clean.groupby(["ddt", "quantile"], observed=False)["M_RETURN"].mean().unstack(fill_value=0)
        # 한 날짜 아닌 시계열 수익률은 기하 수익률로 수정 필요 (? 하드코딩)
        q_mean = q_ret.mean(axis=0).to_frame("mean")
        thresh = abs(q_mean.loc["Q1", "mean"] - q_mean.loc["Q5", "mean"]) * 0.10
        # >= <=
        q_mean["long"] = (q_mean["mean"] > q_mean.loc["Q1", "mean"] - thresh).astype(int).cumprod()
        q_mean["short"] = (q_mean["mean"] < q_mean.loc["Q5", "mean"] + thresh).astype(int) * -1
        q_mean["short"] = q_mean["short"].abs()[::-1].cumprod()[::-1] * -1
        q_mean["label"] = q_mean["long"] + q_mean["short"]

        # Optimization: Use map instead of merge
        label_map = q_mean["label"].to_dict()  # {Q1: 1, Q2: 0, ...}
        # raw_clean은 이미 merged_df의 일부이므로 quantile 컬럼이 있음 (Category or Object)
        # map을 위해 필요한 경우 str로 변환하거나 인덱스 맞춤

        # q_mean 인덱스가 quantile인지 확인 (groupby 결과이므로 인덱스임)
        # raw_clean에 label 컬럼 직접 할당
        raw_clean["label"] = raw_clean["quantile"].map(label_map)
        merged = raw_clean.dropna(subset=["label"])  # merge 동작(inner join)과 유사하게 매칭 안되는 것 제외 (혹시나 해서)

        kept_factor_abbrs.append(factor_abbr_list[idx])
        kept_name.append(factor_name_list[idx])
        kept_style.append(style_name_list[idx])
        kept_idx.append(idx)
        dropped_sec.append(to_drop)
        filtered_raw_data_list.append(merged)

    logger.info("Sector filter retained %d / %d factors", len(kept_idx), len(factor_abbr_list))
    return kept_factor_abbrs, kept_name, kept_style, kept_idx, dropped_sec, filtered_raw_data_list


# =============================================================================
# 3️⃣ 수익률 행렬 · 순위 · 하락 상관관계
# =============================================================================
def calculate_downside_correlation(df: pd.DataFrame, min_obs: int = 20) -> pd.DataFrame:
    """음의 수익률 기간 동안의 상관관계 계산(Downside Correlation 에 가깝지만 기준 자산 수익률이 음수인 모든 기간을 포함)"""
    out = pd.DataFrame(index=df.columns, columns=df.columns, dtype=float)
    for col in df.columns:
        mask = df[col] < 0
        out.loc[col] = df.loc[mask].corr()[col] if mask.sum() >= min_obs else np.nan
    return out

def construct_long_short_df(
    labeled_data_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    raw_df = labeled_data_df[labeled_data_df["ddt"] >= "2017-12-31"].reset_index(drop=True).copy()
    raw_df["signal"] = raw_df["label"].map({1: "L", 0: "N", -1: "S"})
    raw_df["num"] = raw_df.groupby(["ddt", "signal"])["signal"].transform("count")
    # return_weight은 포트폴리오 비중임, 수익률 계산용 #?
    raw_df["return_weight"] = 1 / raw_df["num"] * raw_df["label"]
    # tvr_df 는 턴오버 계산용 wgt임. 실제 턴오버는 trading_friction #?
    raw_df["turnover_weight"] = abs(raw_df["return_weight"])
    long_df = raw_df[raw_df["signal"] == "L"].reset_index(drop=True)
    short_df = raw_df[raw_df["signal"] == "S"].reset_index(drop=True)
    return long_df, short_df


def calculate_vectorized_return(
    portfolio_data_df: pd.DataFrame,
    factor_abbr: str,
    cost_bps: float = 30.0
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    weight_matrix_df = portfolio_data_df.pivot_table(index="ddt", columns="gvkeyiid", values="return_weight")
    rtn_df = portfolio_data_df.pivot_table(index="ddt", columns="gvkeyiid", values="M_RETURN")
    rtn_df.iloc[0] = 0
    # tvr_df 는 턴오버 계산용 wgt임. 실제 턴오버는 trading_friction #?
    turnover_weight_df = portfolio_data_df.pivot_table(index="ddt", columns="gvkeyiid", values="turnover_weight")
    sgn_df = np.sign(weight_matrix_df)

    r = rtn_df.sort_index()
    w = turnover_weight_df.reindex(r.index)
    w0 = turnover_weight_df.copy()
    is_rebal = w.notna().any(axis=1).fillna(False)  # 각 날짜별 NA가 아닌게 하나라도 있으면 True #?
    block_id = is_rebal.cumsum().astype(int)  # 리밸런싱 블럭 1,2,3,.... #?
    cumulative_growth_block = (1 + sgn_df * r).groupby(block_id).cumprod()  # 블럭내에서 누적 곱

    denom = (w0 * cumulative_growth_block).sum(axis=1)  # 각 날짜 비중 합
    w_pre = (w0 * cumulative_growth_block).div(denom, axis=0)  # 각 날짜 비중 100%로 조정

    weight_matrix_df.iloc[0] = w0.loc[weight_matrix_df.index[0]]  # 첫날 비중
    rebal_in_r = r.index.intersection(turnover_weight_df.index)  # 리밸런싱 웨이트와 수익률 날짜(인덱스) 교집합으로 리밸런싱 날짜 선택
    turnover = 1 * (w.shift(-1).loc[rebal_in_r] - w_pre.loc[rebal_in_r]).abs().sum(axis=1)  # 리밸런싱 날짜의 웨이트 차이
    turnover = turnover.reindex(r.index).fillna(0)  # 리밸런싱 날짜 외의 날짜는 0으로 채움
    trading_friction = (cost_bps / 1e4) * turnover  # 거래비용

    _gross = (weight_matrix_df * r).sum(axis=1)  # 날짜별 수익률 (이미 시프트 되어 있음)
    gross_return_df = _gross.to_frame().rename(columns={0: factor_abbr})  # 날짜별 수익률(거래비용 차감전)

    trading_cost_df = trading_friction.to_frame().rename(columns={0: factor_abbr})  # 날짜별 거래비용, 시리즈를 데이터프레임으로 변환
    _net_df = gross_return_df - trading_cost_df  # 날짜별 수익률(거래비용 차감전) - 거래비용

    return gross_return_df, _net_df, trading_cost_df


def aggregate_factor_returns(
    factor_data_list: List[pd.DataFrame],
    factor_abbr_list: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    list_grs, list_net, list_trc = [], [], []
    for list_raw, factor_abbr in zip(factor_data_list, factor_abbr_list):
        long_port_df, short_port_df = construct_long_short_df(list_raw)
        res_grs_l, res_net_l, res_trc_l = calculate_vectorized_return(long_port_df, factor_abbr)
        res_grs_s, res_net_s, res_trc_s = calculate_vectorized_return(short_port_df, factor_abbr)
        list_grs.append(res_grs_l + res_grs_s)
        list_net.append(res_net_l + res_net_s)
        list_trc.append(res_trc_l + res_trc_s)

    gross_return_df = pd.concat(list_grs, axis=1).dropna(axis=1)
    net_return_df = pd.concat(list_net, axis=1).dropna(axis=1)
    trading_cost_df = pd.concat(list_trc, axis=1).dropna(axis=1)

    return gross_return_df, net_return_df, trading_cost_df


def evaluate_factor_universe(
    factor_abbr_list: List[str],
    factor_name_list: List[str],
    style_name_list: List[str],
    factor_data_list: List[pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info("Building monthly return matrix")
    ret_df = aggregate_factor_returns(factor_data_list, factor_abbr_list)[1]
    ret_df.loc[ret_df.index[0]] = 0.0
    ret_df = ret_df.sort_index()  # 날짜 오름차순, 혹시나 싶어서 함 #?

    valid = ret_df.columns[(ret_df == 0).sum() <= 10]  # 컬럼이 팩터, 수익률 0인 애들이 10개 이하인 팩터가 valid
    ret_df = ret_df[valid]

    meta = (
        pd.DataFrame({"factorAbbreviation": factor_abbr_list, "factorName": factor_name_list, "styleName": style_name_list})
        .set_index("factorAbbreviation")
        .loc[valid]
        .reset_index()
    )  # 롱 형식

    months = len(ret_df) - 1
    meta["cagr"] = ((1 + ret_df).cumprod().iloc[-1] ** (12 / months) - 1).values
    meta["rank_style"] = meta.groupby("styleName")["cagr"].rank(ascending=False)  # 스타일내에서의 랭크
    meta["rank_total"] = meta["cagr"].rank(ascending=False)  # 전체에서의 랭크
    # CAGR 내림차순 정렬
    meta = meta.sort_values("cagr", ascending=False).reset_index(drop=True).rename(columns={"index": "factorAbbreviation"})
    meta.to_csv(OUTPUT_DIR / "meta_data.csv", index=False)
    meta = meta[:50]  # 상위 50개만 선택

    order = meta["factorAbbreviation"].tolist()
    ret_df = ret_df[order]  # 50개 팩터만
    negative_corr = calculate_downside_correlation(ret_df).loc[order, order]  # 50개 팩터간의 하락 상관계수

    logger.info("Return matrix built (%d factors)", len(order))
    logger.info(f"[Trace] Generated Factor Return Matrix. Shape: {ret_df.shape}")
    logger.info(f"[Trace] Generated Negative Correlation Matrix. Shape: {negative_corr.shape}")
    return ret_df, negative_corr, meta


# =============================================================================
# 4️⃣ 2-팩터 믹스 최적화
# =============================================================================
def find_optimal_mix(
    factor_rets: pd.DataFrame,
    data_raw: pd.DataFrame,
    data_neg: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[pd.Series], str, float, str, float]:
    """
    메인/서브 팩터 쌍에 대한 최적 가중치 분할을 그리드 탐색

    Returns
    -------
    df_mix : pd.DataFrame
        Grid of weight pairs with performance metrics and rankings.
    ports: List[pd.Series]
        List of mix return series (one per grid column, order aligned with "df_mix").
    main_factor, main_w, sub_factor, sub_w : str | float
        Identifiers and optimal weights.
    """

    # 1. Build candidate list (five sub-factors with best combined rank)
    negative_corr = data_neg.loc[data_raw["factorAbbreviation"], :].T.reset_index().reset_index()
    negative_corr.iloc[:, 0] += 1
    negative_corr.columns = ["rank_cagr", "factorAbbreviation", negative_corr.columns[-1]]
    negative_corr["rank_ncorr"] = negative_corr[negative_corr.columns[-1]].rank()
    negative_corr["rank_avg"] = negative_corr["rank_cagr"] * 0.7 + negative_corr["rank_ncorr"] * 0.3
    negative_corr = negative_corr.nsmallest(3, "rank_avg")

    # 2. Prepare weight grid & common variables
    w_grid = np.linspace(0, 1, 101)
    w_inv = 1 - w_grid
    ann = 12 / factor_rets.shape[0]
    main = data_raw["factorAbbreviation"].iat[0]

    frames: List[pd.DataFrame] = []
    mix_series: List[pd.Series] = []

    # 3. Iterate over candidate sub-factors
    for sub in track(
        negative_corr["factorAbbreviation"], description=f"Mixing {main} with sub-factors"
    ):
        port = factor_rets[[main, sub]]
        mix_ret = port[main].to_numpy()[:, None] * w_grid + port[sub].to_numpy()[:, None] * w_inv
        mix_cum = np.cumprod(1 + mix_ret, axis=0)

        df = pd.DataFrame({
            "main_wgt": w_grid,
            "sub_wgt": w_inv,
            "main_cagr": (1 + port[main]).cumprod().iat[-1] ** ann - 1,
            "sub_cagr": (1 + port[sub]).cumprod().iat[-1] ** ann - 1,
            "mix_cagr": mix_cum[-1] ** ann - 1,
            "main_mdd": ((1 + port[main]).cumprod() / (1 + port[main]).cumprod().cummax() - 1).min(),
            "sub_mdd": ((1 + port[sub]).cumprod() / (1 + port[sub]).cumprod().cummax() - 1).min(),
            "mix_mdd": (mix_cum / np.maximum.accumulate(mix_cum, axis=0) - 1).min(axis=0),
            "main_factor": main,
            "sub_factor": sub
        })
        frames.append(df)

        # Store each mix return column as Series
        mix_series.extend(
            pd.Series(mix_ret[:, i], index=port.index) for i in range(mix_ret.shape[1])
        )
        logger.info("Completed main=%s ↔ sub=%s", main, sub)

    # 4. Concatenate grid & rank
    df_mix = pd.concat(frames, ignore_index=True)
    df_mix["rank_total"] = df_mix["mix_cagr"].rank(ascending=False) * 0.6 + df_mix["mix_mdd"].rank(ascending=False) * 0.4
    logger.info(f"[Trace] Generated Mix Grid for {main}. Size: {len(df_mix)}")
    best = df_mix.nsmallest(1, "rank_total").iloc[0]

    return (
        df_mix,
        mix_series,
        main,
        round(best["main_wgt"], 2),
        best["sub_factor"],
        round(best["sub_wgt"], 2),
    )


# =============================================================================
# 5️⃣ 스타일 포트폴리오 조립
# =============================================================================
def construct_style_portfolios(
    factor_rets: pd.DataFrame,
    meta: pd.DataFrame,
    neg_corr: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """각 스타일별 1위 팩터 선택 및 최적 믹스 시계열 생성"""

    tag_map = {
        "Analyst Expectations": "ane",
        "Price Momentum": "mom",
        "Valuation": "val",
        "Historical Growth": "hig",
        "Capital Efficiency": "caf",
        "Earnings Quality": "eaq",
    }

    mixes: Dict[str, pd.Series] = {}
    processed: set[str] = set()

    for _, row in meta.iterrows():  # meta already sorted by global rank
        style = row["styleName"]
        if style in processed:
            continue
        processed.add(style)
        tag = tag_map.get(style, style[:3].lower())

        df_mix, series_list, *_ = find_optimal_mix(
            factor_rets, row.to_frame().T.reset_index(drop=True), neg_corr
        )
        best_idx = df_mix.nsmallest(1, "rank_total").index[0]
        mixes[tag] = series_list[best_idx].rename(tag)

    style_df = pd.concat(mixes.values(), axis=1)
    style_neg_corr = calculate_downside_correlation(style_df)
    logger.info("Built %d style portfolios", style_df.shape[1])
    return style_df, style_neg_corr


# =============================================================================
# 6️⃣ 팩터 노출도 시뮬레이션
# =============================================================================
def simulate_constrained_weights(
    rtn_df: pd.DataFrame,
    style_list: List[str],
    num_sims: int = 1_000_000,
    style_cap: float = 0.25,
    tol: float = 1e-12,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    스타일 weight 제약이 있는 최적 포트폴리오를 몬테카를로 탐색

    Parameters
    ----------
    rtn_df : DataFrame
        Monthly spread return matrix (rows = dates, cols = factorAbbreviation).
    style_list : list[str]
        Style name for every column in `rtn_df`, in the same order.
    num_sims : int, default 20_000_000
        Number of random portfolios to draw.
    style_cap : float, default 0.25
        Maximum weight share per style.
    tol : float, default 1e-12
        Numerical tolerance when checking caps.

    Returns
    -------
    best_stats : DataFrame (1 × 4)
        CAGR, MDD and rank metrics of the top portfolio.
    weights_tbl : DataFrame
        Columns: factor, raw_weight, styleName, fitted_weight
    """

    # ------------------------------------------------------------------
    # 0. Basic checks & prep
    # ------------------------------------------------------------------
    K = rtn_df.shape[1]
    if len(style_list) != K:
        raise ValueError("length of style_list must equal number of columns in rtn_df")

    styles = np.asarray(style_list)

    # ------------------------------------------------------------------
    # 1. Random raw weights (Σ w = 1 per column)
    # ------------------------------------------------------------------
    raw_mat = np.random.rand(K, num_sims).astype(np.float64)
    raw_mat /= raw_mat.sum(axis=0, keepdims=True)

    # ------------------------------------------------------------------
    # 2. Build style mask (S × K)
    # ------------------------------------------------------------------
    uniq_styles = np.unique(styles)
    S = len(uniq_styles)

    mask = np.zeros((S, K), dtype=int)
    for i, s in enumerate(uniq_styles):
        mask[i, styles == s] = 1

    # ------------------------------------------------------------------
    # 3. Apply style caps (shrink & redistribute)
    # ------------------------------------------------------------------
    share = mask @ raw_mat
    excess = np.clip(share - style_cap, a_min=0, a_max=None)
    scale = np.where(share > style_cap, style_cap / share, 1.0)
    shrink = mask.T @ scale
    mat_scaled = raw_mat * shrink

    room = np.where(share < style_cap, style_cap - share, 0)
    room_sum = room.sum(axis=0, keepdims=True)
    ratio = np.divide(room, room_sum, where=room_sum != 0)
    add = excess.sum(axis=0, keepdims=True) * ratio
    fitted_mat = mat_scaled + (mask.T @ add)
    fitted_mat /= fitted_mat.sum(axis=0, keepdims=True)

    # Feasibility filter
    ok = (mask @ fitted_mat <= style_cap + tol).all(axis=0)
    raw_mat = raw_mat[:, ok]
    fitted_mat = fitted_mat[:, ok]

    # ------------------------------------------------------------------
    # 4. Simulate returns
    # ------------------------------------------------------------------
    port_np = rtn_df.to_numpy(dtype=np.float32)
    sim = port_np @ fitted_mat                         # (T × sims)
    sim = pd.DataFrame(sim, index=rtn_df.index)

    ann_exp = 12 / len(sim)
    cum = (1 + sim).cumprod()
    cagr = cum.iloc[-1].pow(ann_exp) - 1
    mdd = (cum / cum.cummax() - 1).min()

    stats = pd.DataFrame({"cagr": cagr, "mdd": mdd})
    stats["rank_cagr"] = stats["cagr"].rank(ascending=False)
    stats["rank_mdd"] = stats["mdd"].rank(ascending=False)
    stats["rank_total"] = stats["rank_cagr"] * 0.6 + stats["rank_mdd"] * 0.4

    # ------------------------------------------------------------------
    # 5. Pick the best portfolio & build weight table
    # ------------------------------------------------------------------
    best_idx = stats["rank_total"].idxmin()
    best_stats = stats.loc[[best_idx]]

    factors = rtn_df.columns.to_numpy()

    # ~2025-12 포트폴리오까지 적용
    # weights_tbl = pd.DataFrame({
    #     "factor": np.array(['SalesAcc', '6MTTMSalesMom', 'PM6M', '52WSlope', '90DCV',
    #         'CashEV', 'RevMagFY1C', 'SalesToEPSChg', 'Rev3MFY1C', 'TobinQ']),
    #     "raw_weight": np.array([0.199298652556654,0.00842206236153488,0.196025173956866,0.0326737859629076,0.174696911741135,
    #         0.148243451062375,0.10775464398236,0.0577835874187986,0.0524125911883854,0.022689139768980]),
    #     "styleName": np.array(['Historical Growth', 'Historical Growth', 'Price Momentum', 'Price Momentum', 'Volatility',
    #         'Valuation', 'Analyst Expectations', 'Earnings Quality', 'Analyst Expectations', 'Capital Efficiency']),
    #     "fitted_weight": np.array([0.199298652556654,0.00842206236153488,0.196025173956866,0.0326737859629076,0.174696911741135,
    #         0.148243451062375,0.10775464398236,0.0577835874187986,0.0524125911883854,0.022689139768980]),
    # })

    # # 2025-11-30 기준으로 계산한 팩터 비중
    # weights_tbl = pd.DataFrame({
    #     "factor": np.array([
    #         'Rev3MFY2C', 'RevMagFY1C', 'TobinQ', 'FCFSales', 'SalesAcc',
    #         '52WSlope', 'PM6M', 'FwdEPC', '90DCV'
    #     ]),
    #     "raw_weight": np.array([
    #         0.021621521816389294, 0.20431602944049673, 0.049936965510287146, 0.09875848294333273, 0.24346249417352991,
    #         0.024010934147244627, 0.20767050160349557, 0.14737045318156372, 0.0028526171836602172
    #     ]),
    #     "styleName": np.array([
    #         'Analyst Expectations', 'Analyst Expectations', 'Capital Efficiency', 'Earnings Quality', 'Historical Growth',
    #         'Price Momentum', 'Price Momentum', 'Valuation', 'Volatility'
    #     ]),
    #     "fitted_weight": np.array([
    #         0.021621521816389294, 0.20431602944049673, 0.049936965510287146, 0.09875848294333273, 0.24346249417352991,
    #         0.024010934147244627, 0.20767050160349557, 0.14737045318156372, 0.0028526171836602172
    #     ]),
    # })

    weights_tbl = pd.DataFrame({
        "factor": factors,
        "raw_weight": raw_mat[:, best_idx],
        "styleName": styles,
        "fitted_weight": fitted_mat[:, best_idx],
    })

    weights_tbl = (
        weights_tbl[weights_tbl["raw_weight"] > 0]
        .sort_values("raw_weight", ascending=False)
        .reset_index(drop=True)
    )

    logger.info(f"[Trace] Simulation completed. Best stats: {best_stats.to_dict('records')}")
    return best_stats, weights_tbl


def run_model_portfolio_pipeline(start_date, end_date, report: bool = False, test_file: str | None = None) -> None:
    # parquet 파일 또는 테스트 CSV 파일 로드하기
    t0 = time.time()
    if test_file:
        import re
        test_data_path = Path.cwd() / test_file
        raw_factor_data_df = pd.read_csv(test_data_path)
        # fld 컬럼에서 factorAbbreviation 추출 (예: "Sales Acceleration (SalesAcc)" -> "SalesAcc")
        def extract_abbr(fld_value):
            match = re.search(r'\(([^)]+)\)$', fld_value)
            return match.group(1) if match else fld_value
        raw_factor_data_df['factorAbbreviation'] = raw_factor_data_df['fld'].apply(extract_abbr)
        raw_factor_data_df = raw_factor_data_df.drop(columns=['fld', 'updated_at'])
        logger.info(f"Query loaded from {test_data_path} in {time.time() - t0:.2f}s")
        logger.info(f"[Trace] Loaded test data. Shape: {raw_factor_data_df.shape}")
    else:
        parquet_path = DATA_DIR / f"{PARAM['benchmark']}_{start_date}_{end_date}.parquet"
        raw_factor_data_df = pd.read_parquet(parquet_path)
        logger.info(f"Query loaded from {parquet_path} in {time.time() - t0:.2f}s")
        logger.info(f"[Trace] Loaded parquet data. Shape: {raw_factor_data_df.shape}")

    # 2️⃣ 메타데이터(순서/스타일/이름)와 조인
    t1 = time.time()
    factor_metadata_df = pd.read_csv(DATA_DIR / "factor_info.csv")
    merged_factor_data_df = raw_factor_data_df.merge(factor_metadata_df, on="factorAbbreviation", how="inner")
    logger.info(f"[Trace] Merged with factor info. Shape: {merged_factor_data_df.shape}")

    factor_abbr_list, orders = factor_metadata_df.factorAbbreviation.tolist(), factor_metadata_df.factorOrder.tolist()

    # 3️⃣ Rich 진행바와 함께 팩터 할당
    # 최적화: M_RETURN 미리 추출
    market_return_df = (
        raw_factor_data_df[raw_factor_data_df["factorAbbreviation"] == "M_RETURN"].reset_index(drop=True)
        .rename(columns={"val": "M_RETURN"})
        .drop(columns=["factorAbbreviation"])
    )
    logger.info(f"[Trace] Extracted M_RETURN. DDT distinct: {market_return_df['ddt'].nunique()}, GVKeyIID distinct: {market_return_df['gvkeyiid'].nunique()}")

    # 최적화: M_RETURN을 전체 데이터에 병합 (Loop 밖에서 수행)
    # 이를 통해 calculate_factor_stats 내부의 반복적인 병합을 제거함
    merged_factor_data_df = (
        merged_factor_data_df
        .merge(
            market_return_df,
            on=["gvkeyiid", "ticker", "isin", "ddt", "sec", "country"],
            how="inner",
        )
        .query("sec != 'Undefined'")  # 정의되지 않은 섹터 전역 필터링
    )
    logger.info(f"[Trace] Merged M_RETURN globally. Shape: {merged_factor_data_df.shape}")

    # 최적화: meta를 미리 그룹화
    grouped_source_data = merged_factor_data_df.groupby("factorAbbreviation")
    logger.info(f"[Trace] Grouped source data. Number of groups: {grouped_source_data.ngroups}")

    processed_factor_data_list: List[Any] = []
    for factor_abbr, order in track(zip(factor_abbr_list, orders), total=len(factor_abbr_list), description="Assigning factors"):
        # 그룹이 존재하면 가져오고, 없으면 빈 DataFrame 전달
        if factor_abbr in grouped_source_data.groups:
            factor_data_df = grouped_source_data.get_group(factor_abbr).copy()  # copy to avoid SettingWithCopy
        else:
            factor_data_df = pd.DataFrame(columns=merged_factor_data_df.columns)

        # market_return_df 인자 제거 (이미 병합됨)
        processed_factor_data_list.append(calculate_factor_stats(factor_abbr, order, factor_data_df))
    logger.info(f"Factors assigned in {time.time() - t1:.2f}s")

    """Run the full ETL → optimisation → export process."""
    logger.info("Report generation started for period: %s to %s", start_date, end_date)

    # 1. 피클 로드 및 섹터 필터 적용
    factor_abbr_list, factor_name_list, style_name_list, raw = factor_abbr_list, factor_metadata_df.factorName.tolist(), factor_metadata_df.styleName.tolist(), processed_factor_data_list

    if report:
        from service.report.read_pkl import generate_report
        import sys
        logger.info("Report generation requested. Invoking read_pkl logic...")
        generate_report(factor_abbr_list, factor_name_list, style_name_list, raw)
        logger.info("Report generated. Exiting.")
        sys.exit(0)
    kept_factor_abbrs, kept_name, kept_style, _, _, filtered_factor_data_list = filter_and_label_factors(factor_abbr_list, factor_name_list, style_name_list, raw)

    # 2. 수익률 행렬, 음의 상관관계 행렬, 메타 순위 테이블 생성
    monthly_return_matrix, downside_correlation_matrix, factor_performance_metrics = evaluate_factor_universe(kept_factor_abbrs, kept_name, kept_style, filtered_factor_data_list)

    # 3. 각 스타일의 최상위 팩터(팩터들?)에 대해서만 가중치 그리드 생성
    top_metrics = factor_performance_metrics.groupby("styleName", as_index=False).first()  # 스타일별 최상위 팩터
    grids = []
    for _, row in top_metrics.iterrows():  # .iterrows() 가 인덱스, 값으로 반환
        grid, *_ = find_optimal_mix(monthly_return_matrix, row.to_frame().T.reset_index(drop=True), downside_correlation_matrix)
        grid["styleName"] = row["styleName"]
        grids.append(grid)
    mix_grid = pd.concat(grids, ignore_index=True)

    # 선택된 팩터로 수익률 행렬 부분집합 생성
    # 5. 각 메인 팩터에 대한 최적 서브 팩터 선택 ── 스타일 이름 추가
    best_sub = (
        mix_grid.sort_values("rank_total")  # ascending by rank_total
        .groupby("main_factor", as_index=False)  # group by each main_factor
        .first()[["main_factor", "sub_factor"]]  # keep the smallest-rank row
    )

    # ------------------------------------------------------------------
    # Map factor → style and append to best_sub
    # ------------------------------------------------------------------
    style_map = factor_performance_metrics.set_index("factorAbbreviation")["styleName"]
    best_sub["main_style"] = best_sub["main_factor"].map(style_map)
    best_sub["sub_style"] = best_sub["sub_factor"].map(style_map)
    best_sub = best_sub[["main_factor", "main_style", "sub_factor", "sub_style"]]

    # Save if needed
    # best_sub.to_csv(OUTPUT_DIR / "best_sub_factor.csv", index=False)

    # 6. 메인 팩터와 보조 팩터로 수익률 행렬 합집합 생성
    cols_to_keep = pd.unique(best_sub[["main_factor", "sub_factor"]].to_numpy().ravel())
    ret_subset = monthly_return_matrix[cols_to_keep]

    # 7. factor_list 및 style_list 구성 (정렬된 순서)
    factor_list = pd.unique(best_sub[["main_factor", "sub_factor"]].to_numpy().ravel()).tolist()
    style_list = [style_map[f] for f in factor_list]

    sim_result = simulate_constrained_weights(ret_subset, style_list)

    # ------------------------------------------------------------------
    # 8. 팩터별 가중치 테이블 구성 (date × id × weight)
    # ------------------------------------------------------------------
    weight_frames = []
    for _, row in sim_result[1].iterrows():
        fac = row['factor']
        w = row['fitted_weight']
        s = row['styleName']

        j = kept_factor_abbrs.index(fac)
        df = filtered_factor_data_list[j][['ddt', 'ticker', 'isin', 'gvkeyiid', 'label']].copy()
        df['weight'] = df['label'] * w / df.groupby(['ddt', 'label'])['label'].transform('count')
        df['ls_weight'] = df['label'] / df.groupby(['ddt', 'label'])['label'].transform('count')
        df['factor_weight'] = w
        df['style'] = s
        df['name'] = f'MXCN1A_{s}'
        df['factor'] = fac
        df['count'] = df.groupby(['ddt', 'label'])['label'].transform('count')
        df["ticker"] = df["ticker"].astype(str).str.zfill(6).add(" CH Equity")
        end_date_df = df[df['ddt'] == end_date].reset_index(drop=True)
        weight_frames.append(end_date_df[['ddt', 'ticker', 'isin', 'gvkeyiid', 'weight',
                                          'ls_weight', 'factor_weight',
                                          'factor', 'style', 'name', 'count']])

    # ------------------------------------------------------------------
    # 9. 팩터 간 집계 (Σ weights per date × security)
    # ------------------------------------------------------------------
    weight_raw = pd.concat(weight_frames, ignore_index=True)
    # weight_raw = weight_raw[weight_raw['factor'] != 'SalesAcc'].reset_index(drop=True)
    weight_raw['factor_weight'] = weight_raw['factor_weight'] * np.sign(weight_raw['weight']) ** 2
    agg_w = (
        weight_raw
        .groupby(["ddt", "ticker", "isin", "gvkeyiid"], as_index=False)["weight"]
        .sum()
    )

    # ▶︎ zero-pad tickers to 6 chars
    # agg_w["ticker"] = agg_w["ticker"].astype(str).str.zfill(6).add(" CH Equity")
    agg_w['style'] = 'MP'
    factor_sum = (
        weight_raw
        .groupby(["ddt", "ticker", "isin", "gvkeyiid"], as_index=False)["factor_weight"]
        .sum()
    )
    agg_w = agg_w.merge(
        factor_sum,
        on=["ddt", "ticker", "isin", "gvkeyiid"],
        how="left"
    )
    agg_w['name'] = 'MXCN1A_MP'
    agg_w = agg_w[agg_w['ddt'] == end_date].reset_index(drop=True)
    agg_w['count'] = (
        agg_w.groupby(['ddt', agg_w['weight'] > 0])['weight']
        .transform('size')
    )
    agg_w['factor'] = 'AGG'
    agg_w = agg_w[['ddt', 'ticker', 'isin', 'gvkeyiid', 'weight', 'factor_weight', 'factor', 'style', 'name', 'count']]

    weight_raw = weight_raw.drop(columns=['weight'])
    weight_raw = weight_raw.rename(columns={'ls_weight': 'weight'})
    final_weights = pd.concat([weight_raw, agg_w],
                              axis=0,
                              ignore_index=True)

    final_style_weight = (
        final_weights.groupby(['ddt', 'ticker', 'isin', 'gvkeyiid', 'style'])[['weight', 'factor_weight']]
        .sum()
    )

    # 테스트 파일이 제공된 경우, 파일명(확장자 제외)을 suffix로 사용
    suffix = f"_{Path(test_file).stem}" if test_file else ""
    agg_w.to_csv(OUTPUT_DIR / f"aggregated_weights_{end_date}_test{suffix}.csv")
    final_weights.to_csv(OUTPUT_DIR / f"total_aggregated_weights_{end_date}_test{suffix}.csv")

    final_style_weight.to_csv(OUTPUT_DIR / f"total_aggregated_weights_style_{end_date}_test{suffix}.csv")

    final_weights.loc[final_weights['style'] == 'MP', 'factor_weight'] = 1
    final_weights = final_weights.replace(0, np.nan)

    pivoted_final = final_weights.pivot_table(
        index=['ddt', 'ticker', 'isin', 'gvkeyiid'],
        columns=['style', 'factor_weight', 'factor'],
        values='weight',
        aggfunc='sum'
    ).reset_index()

    sample_df = pd.DataFrame({"factor": pivoted_final.columns.get_level_values(2).tolist()[4:]})
    sum_df = pd.merge(sim_result[1], sample_df, on='factor', how='inner')

    final_weights.loc[final_weights['style'] == 'MP', 'factor_weight'] = sum_df['fitted_weight'].sum(axis=0)
    final_weights = final_weights.replace(0, np.nan)

    pivoted_final = final_weights.pivot_table(
        index=['ddt', 'ticker', 'isin', 'gvkeyiid'],
        columns=['style', 'factor_weight', 'factor'],
        values='weight',
        aggfunc='sum'
    ).reset_index()

    cols = pivoted_final.columns
    mp_mask = cols.get_level_values('style') == 'MP'

    new_order = cols[~mp_mask].tolist() + cols[mp_mask].tolist()
    pivoted_final = pivoted_final.loc[:, new_order]

    pivoted_final.to_csv(OUTPUT_DIR / f"pivoted_total_agg_wgt_{end_date}{suffix}.csv")
    logger.info("Pipeline completed ✓ — files saved in %s", OUTPUT_DIR)