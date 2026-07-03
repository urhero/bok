# -*- coding: utf-8 -*-
"""종목 수준의 롱/숏 포지션 구분 및 포트폴리오 수익률 계산 모듈.

filter_and_label_factors()에서 L/N/S 라벨이 부여된 종목 데이터를 받아,
롱/숏으로 분리하고 동일가중 포트폴리오의 수익률과 거래비용을 계산한다.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def build_factor_weight_frames(
    sim_factors: list[dict],
    kept_abbrs: list[str],
    filtered_data: list[pd.DataFrame],
    end_date_ts: pd.Timestamp,
) -> pd.DataFrame | None:
    """팩터별 종목 가중치 프레임을 생성하고 결합한다.

    각 팩터의 라벨링된 종목 데이터에서 end_date 기준 가중치를 계산하고,
    neutral 종목의 factor_weight를 0으로 처리한 후 결합한다.

    Args:
        sim_factors: 시뮬레이션 결과 팩터 목록 (factor, fitted_weight, styleName)
        kept_abbrs: 유지된 팩터 약어 목록
        filtered_data: 팩터별 필터링된 DataFrame 목록
        end_date_ts: 기준 날짜 Timestamp

    Returns:
        결합된 가중치 DataFrame, 또는 매칭 팩터가 없으면 None
    """
    factor_idx_map = {abbr: idx for idx, abbr in enumerate(kept_abbrs)}
    weight_frames = []
    for row in sim_factors:
        factor_abbr, fitted_weight, style_name = row["factor"], row["fitted_weight"], row["styleName"]

        if factor_abbr not in factor_idx_map:
            logger.warning("Factor %s not in filtered data, skipping", factor_abbr)
            continue

        factor_idx = factor_idx_map[factor_abbr]
        # end_date를 먼저 필터하여 이후 연산 대상 행 수를 최소화
        df = filtered_data[factor_idx].loc[
            filtered_data[factor_idx]["ddt"] == end_date_ts, ["ddt", "ticker", "isin", "gvkeyiid", "label"]
        ].copy()
        if df.empty:
            logger.warning("Factor %s has no stock data at %s, skipping (book gross may dip below 1.0)",
                           factor_abbr, end_date_ts.date())
            continue
        count_per_group = df.groupby("label")["label"].transform("count")

        df["mp_ls_weight"] = df["label"] * fitted_weight / count_per_group
        df["ls_weight"] = df["label"] / count_per_group
        df["factor_weight"] = fitted_weight
        df["style"] = style_name
        df["name"] = f"MXCN1A_{style_name}"
        df["factor"] = factor_abbr
        df["count"] = count_per_group
        df["ticker"] = df["ticker"].astype(str).str.zfill(6).add(" CH Equity")

        weight_frames.append(df[["ddt", "ticker", "isin", "gvkeyiid", "mp_ls_weight", "ls_weight", "factor_weight", "factor", "style", "name", "count"]].reset_index(drop=True))

    if not weight_frames:
        logger.warning("No matching factors found in filtered data - skipping CSV export")
        return None

    weight_raw = pd.concat(weight_frames, ignore_index=True)
    # neutral 종목(mp_ls_weight=0)의 factor_weight를 0으로 처리
    weight_raw["factor_weight"] = weight_raw["factor_weight"] * (weight_raw["mp_ls_weight"] != 0).astype(int)
    return weight_raw


def aggregate_mp_weights(
    weight_raw: pd.DataFrame,
    end_date_ts: pd.Timestamp,
) -> pd.DataFrame:
    """MP(Model Portfolio, 전체 팩터 합산) 가중치를 생성한다.

    Args:
        weight_raw: build_factor_weight_frames() 결과
        end_date_ts: 기준 날짜 Timestamp

    Returns:
        MP 집계 가중치 DataFrame
    """
    agg_w = weight_raw.groupby(["ddt", "ticker", "isin", "gvkeyiid"], as_index=False)[["mp_ls_weight", "factor_weight"]].sum()
    agg_w["style"] = "MP"
    agg_w["name"] = "MXCN1A_MP"
    agg_w = agg_w[agg_w["ddt"] == end_date_ts].reset_index(drop=True)
    agg_w["count"] = agg_w.groupby(["ddt", agg_w["mp_ls_weight"] > 0])["mp_ls_weight"].transform("size")
    agg_w["factor"] = "AGG"
    agg_w["ls_weight"] = agg_w["mp_ls_weight"]
    agg_w = agg_w[["ddt", "ticker", "isin", "gvkeyiid", "mp_ls_weight", "ls_weight", "factor_weight", "factor", "style", "name", "count"]]
    return agg_w


def calculate_style_weights(
    weight_raw: pd.DataFrame,
) -> pd.DataFrame:
    """스타일별 ls_weight를 계산한다.

    non-zero factor_weight를 가진 종목에 대해 스타일별 합산 비중으로
    정규화된 style_ls_weight를 계산한다.

    Args:
        weight_raw: build_factor_weight_frames() 결과

    Returns:
        style_ls_weight 컬럼이 추가된 DataFrame
    """
    non_zero_fw = weight_raw[weight_raw["factor_weight"] > 0]
    unique_factor_fw = non_zero_fw.groupby(["ddt", "style", "factor"])["factor_weight"].first().reset_index()
    style_totals = unique_factor_fw.groupby(["ddt", "style"], as_index=False)["factor_weight"].sum()
    style_totals = style_totals.rename(columns={"factor_weight": "_style_fw_sum"})
    weight_raw = weight_raw.merge(style_totals, on=["ddt", "style"], how="left")
    weight_raw["_style_fw_sum"] = weight_raw["_style_fw_sum"].fillna(0)
    weight_raw["style_ls_weight"] = np.where(
        weight_raw["_style_fw_sum"] != 0,
        weight_raw["ls_weight"] * weight_raw["factor_weight"] / weight_raw["_style_fw_sum"],
        0,
    )
    weight_raw = weight_raw.drop(columns=["_style_fw_sum"])
    return weight_raw


def build_pivoted_export(final_weights: pd.DataFrame, sim_result) -> pd.DataFrame:
    """MP factor_weight 백필 후 (style, factor_weight, factor) 피벗 테이블을 만든다.

    _construct_and_export 의 피벗 단계를 추출한 것이다. round(12) 와 MP 컬럼
    후위 재정렬은 결정적 출력(git diff 안정화)을 위한 load-bearing 가드이므로
    연산 순서를 그대로 보존한다.

    Args:
        final_weights: weight_raw + agg_w 결합 프레임 (style/factor/ls_weight 등 포함).
        sim_result: (best_stats, weights_tbl) 튜플. weights_tbl(=sim_result[1])의
            fitted_weight 합으로 MP 행 factor_weight 를 채운다.

    Returns:
        피벗 테이블 (MP 컬럼이 맨 뒤로 재정렬됨).
    """
    mp_mask = final_weights["style"] == "MP"
    factors_in_data = final_weights.loc[~mp_mask, "factor"].unique()
    matched_weights = sim_result[1][sim_result[1]["factor"].isin(factors_in_data)]
    final_weights.loc[mp_mask, "factor_weight"] = matched_weights["fitted_weight"].sum()
    final_weights = final_weights.replace(0, np.nan)
    # 결정적 출력: factor_weight(피벗 컬럼 키)의 말단 부동소수점 표기 변동 제거.
    # MP factor_weight = 37개 가중치 합 ~= 1.0 인데 합산 순서에 따라 0.999..98 / 1.000..02 로
    # 흔들려 피벗 헤더가 실행마다 달라짐 -> 12자리 반올림으로 고정 (값 영향 무시 가능).
    final_weights["factor_weight"] = final_weights["factor_weight"].round(12)

    pivoted_final = final_weights.pivot_table(
        index=["ddt", "ticker", "isin", "gvkeyiid"],
        columns=["style", "factor_weight", "factor"],
        values="ls_weight",
        aggfunc="sum",
    ).reset_index()

    cols = pivoted_final.columns
    mp_mask = cols.get_level_values("style") == "MP"
    new_order = cols[~mp_mask].tolist() + cols[mp_mask].tolist()
    return pivoted_final.loc[:, new_order]


def construct_long_short_df(
    labeled_data_df: pd.DataFrame,
    backtest_start: str = "2017-12-31",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """라벨링된 종목 데이터를 롱(L)/숏(S) 포트폴리오로 분리한다.

    label=1(롱), label=-1(숏) 종목을 분리하고,
    같은 날짜·같은 시그널 내에서 동일가중(equal-weight) 비중을 부여한다.

    Args:
        labeled_data_df: filter_and_label_factors() 결과. label 컬럼 필수.

    Returns:
        (long_df, short_df) 튜플

    예시 Input:
        | ddt        | gvkeyiid | ticker | M_RETURN | label |
        |------------|----------|--------|----------|-------|
        | 2024-01-31 | 001      | 600519 | 0.03     | 1     |
        | 2024-01-31 | 002      | 000858 | -0.01    | -1    |
        | 2024-01-31 | 003      | 601318 | 0.02     | 0     |

    예시 Output (long_df):
        | ddt        | gvkeyiid | ticker | M_RETURN | label | signal | num | return_weight | turnover_weight |
        |------------|----------|--------|----------|-------|--------|-----|---------------|-----------------|
        | 2024-01-31 | 001      | 600519 | 0.03     | 1     | L      | 1   | 1.0           | 1.0             |
    """
    # neutral(label=0)을 먼저 제거 — 이후 연산 대상 행 ~20% 절감
    raw_df = labeled_data_df[(labeled_data_df["ddt"] >= backtest_start) & (labeled_data_df["label"] != 0)].copy()
    raw_df["signal"] = raw_df["label"].map({1: "L", -1: "S"})
    raw_df["num"] = raw_df.groupby(["ddt", "signal"])["signal"].transform("count")
    raw_df["return_weight"] = raw_df["label"] / raw_df["num"]
    raw_df["turnover_weight"] = abs(raw_df["return_weight"])
    long_df = raw_df[raw_df["signal"] == "L"].reset_index(drop=True)
    short_df = raw_df[raw_df["signal"] == "S"].reset_index(drop=True)
    return long_df, short_df


def calculate_vectorized_return(
    portfolio_data_df: pd.DataFrame,
    factor_abbr: str,
    cost_bps: float = 20.0,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """포트폴리오의 총수익률·순수익률·거래비용을 벡터 연산으로 계산한다.

    리밸런싱 시점의 턴오버를 추적하여 거래비용(bps 기반)을 차감한다.

    Args:
        portfolio_data_df: 롱 또는 숏 포트폴리오 (construct_long_short_df 결과)
        factor_abbr: 팩터 약어 (컬럼명으로 사용)
        cost_bps: 거래비용 (basis points, 기본 20bp = 0.20%)

    Returns:
        (gross_return_df, net_return_df, trading_cost_df) 튜플
        각각 (날짜 × 1) DataFrame

    예시 Input:
        portfolio_data_df (long_df):
        | ddt        | gvkeyiid | M_RETURN | return_weight | turnover_weight |
        |------------|----------|----------|---------------|-----------------|
        | 2024-01-31 | 001      | 0.03     | 0.5           | 0.5             |
        | 2024-01-31 | 002      | 0.01     | 0.5           | 0.5             |

    예시 Output:
        gross_return_df:
        | ddt        | SalesAcc |
        |------------|----------|
        | 2024-01-31 | 0.0      |
        | 2024-02-28 | 0.02     |
    """
    # 단일 pivot으로 3개 값을 한번에 추출
    pivoted = portfolio_data_df.pivot_table(
        index="ddt", columns="gvkeyiid", values=["return_weight", "M_RETURN", "turnover_weight"]
    )
    weight_matrix_df = pivoted["return_weight"]
    rtn_df = pivoted["M_RETURN"].copy()
    rtn_df.iloc[0] = 0
    turnover_weight_df = pivoted["turnover_weight"]
    sgn_df = np.sign(weight_matrix_df)

    r = rtn_df.sort_index()
    w = turnover_weight_df.reindex(r.index)
    w0 = turnover_weight_df
    is_rebal = w.notna().any(axis=1).fillna(False)
    block_id = is_rebal.cumsum().astype(int)
    cumulative_growth_block = (1 + sgn_df * r).groupby(block_id).cumprod()

    # w0 * cumulative_growth_block를 한번만 계산
    weighted_growth = w0 * cumulative_growth_block
    denom = weighted_growth.sum(axis=1)
    w_pre = weighted_growth.div(denom, axis=0)

    rebal_in_r = r.index.intersection(turnover_weight_df.index)
    # 편입 매수/편출 매도 포함: 미보유 월(NaN) 비중을 0으로 간주해 |w_next - w_pre|
    # 전액을 턴오버로 계상한다. (구 버전은 NaN 차감이 합산에서 빠져 연속 보유 종목의
    # 비중 변화만 계상 -> 비용 과소, 고회전 팩터가 랭킹에서 과대평가되는 편향)
    turnover = (
        w.shift(-1).loc[rebal_in_r].fillna(0.0) - w_pre.loc[rebal_in_r].fillna(0.0)
    ).abs().sum(axis=1)
    if len(turnover) > 0:
        # 마지막 월은 다음 목표 비중이 없음(청산 아님) -> 비용 0 (기존 동작 유지)
        turnover.iloc[-1] = 0.0
    turnover = turnover.reindex(r.index).fillna(0)
    trading_friction = (cost_bps / 1e4) * turnover

    _gross = (weight_matrix_df * r).sum(axis=1)
    gross_return_df = _gross.to_frame().rename(columns={0: factor_abbr})

    trading_cost_df = trading_friction.to_frame().rename(columns={0: factor_abbr})
    _net_df = gross_return_df - trading_cost_df

    return gross_return_df, _net_df, trading_cost_df
