# -*- coding: utf-8 -*-
"""종목 수준의 롱/숏 포지션 구분 및 포트폴리오 수익률 계산 모듈.

filter_and_label_factors()에서 L/N/S 라벨이 부여된 종목 데이터를 받아,
롱/숏으로 분리하고 동일가중 포트폴리오의 수익률과 거래비용을 계산한다.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from config import PARAM

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
            filtered_data[factor_idx]["ddt"] == end_date_ts, ["ddt", "ticker", "isin", "gvkeyiid", "sec", "label"]
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
        df["name"] = f"{PARAM['benchmark']}_{style_name}"
        df["factor"] = factor_abbr
        df["count"] = count_per_group
        # Bloomberg 티커 포맷은 중국 A주(MXCN1A) 전용. 그 외 유니버스(MXWO 등)는
        # 다국가 로컬 티커라 그대로 두고 ISIN을 식별자로 사용한다.
        if PARAM["benchmark"] == "MXCN1A":
            df["ticker"] = df["ticker"].astype(str).str.zfill(6).add(" CH Equity")

        weight_frames.append(df[["ddt", "ticker", "isin", "gvkeyiid", "sec", "mp_ls_weight", "ls_weight", "factor_weight", "factor", "style", "name", "count"]].reset_index(drop=True))

    if not weight_frames:
        logger.warning("No matching factors found in filtered data - skipping CSV export")
        return None

    weight_raw = pd.concat(weight_frames, ignore_index=True)
    # neutral 종목(mp_ls_weight=0)의 factor_weight를 0으로 처리
    weight_raw["factor_weight"] = weight_raw["factor_weight"] * (weight_raw["mp_ls_weight"] != 0).astype(int)
    return weight_raw


def apply_sector_short_cap(agg_w: pd.DataFrame, cap: float | None) -> pd.DataFrame:
    """숏 crowding 완화 (2026-07-30 채택): 섹터별 숏 gross 가 전체 숏 gross 의
    cap 비율을 넘으면 그 섹터 숏을 줄이고, 잘린 만큼을 다른 섹터 숏에 비례
    재분배한다 (총 숏 gross 보존 — 2020-11형 백신 로테이션 이벤트 리스크 대응).
    mp_level_cost_backtest.stock_weights_at 의 sector_short_cap 과 동일 로직.
    """
    if not cap or agg_w.empty:
        return agg_w
    w = agg_w["mp_ls_weight"]
    shorts = w < 0
    total_sg = w[shorts].abs().sum()
    if total_sg <= 1e-12:
        return agg_w
    sec_gross = w[shorts].abs().groupby(agg_w.loc[shorts, "sec"]).sum()
    over = sec_gross[sec_gross > cap * total_sg]
    if over.empty:
        return agg_w
    agg_w = agg_w.copy()
    freed = 0.0
    for sec, sg in over.items():
        idx = agg_w.index[shorts & (agg_w["sec"] == sec)]
        agg_w.loc[idx, "mp_ls_weight"] *= (cap * total_sg) / sg
        freed += sg - cap * total_sg
    under_idx = agg_w.index[shorts & ~agg_w["sec"].isin(over.index)]
    under_g = agg_w.loc[under_idx, "mp_ls_weight"].abs().sum()
    if under_g > 1e-12 and freed > 0:
        agg_w.loc[under_idx, "mp_ls_weight"] *= 1.0 + freed / under_g
    logger.info("sector_short_cap %.0f%%: %s 섹터 숏 축소, %.2f%%p 재분배",
                cap * 100, list(over.index), freed * 100)
    return agg_w


def aggregate_mp_weights(
    weight_raw: pd.DataFrame,
    end_date_ts: pd.Timestamp,
    sector_short_cap: float | None = None,
) -> pd.DataFrame:
    """MP(Model Portfolio, 전체 팩터 합산) 가중치를 생성한다.

    Args:
        weight_raw: build_factor_weight_frames() 결과
        end_date_ts: 기준 날짜 Timestamp
        sector_short_cap: 섹터별 숏 gross 상한 (전체 숏 gross 대비 비율, None=off)

    Returns:
        MP 집계 가중치 DataFrame
    """
    agg_w = weight_raw.groupby(["ddt", "ticker", "isin", "gvkeyiid", "sec"], as_index=False, observed=True)[["mp_ls_weight", "factor_weight"]].sum()
    agg_w["style"] = "MP"
    agg_w["name"] = f"{PARAM['benchmark']}_MP"
    agg_w = agg_w[agg_w["ddt"] == end_date_ts].reset_index(drop=True)
    agg_w = apply_sector_short_cap(agg_w, sector_short_cap)
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


# MP 배포 배수 (2026-08-19 도입): 최종 MP 북을 실제 포트폴리오에 적용할 때 곱하는
# 배수를 산출물에 미리 반영한다 (Bloomberg ex-ante TE 확인을 배수 적용 상태로 하기 위함).
# data/mp_multiplier.csv 는 (effective_date, multiplier) 이력 — 해당 시점 이후 다음
# 변경 전까지 유효한 계단식 값. 파일/해당 행이 없으면 1.0 (미적용).
MULTIPLIER_COLS = ("mp_ls_weight", "ls_weight", "style_ls_weight")


def multiplier_for_target(book_gross: float, target_gross: float) -> float:
    """목표 총 gross(롱+|숏|)에 맞추는 배수. netting 변동을 흡수해 노출을 고정한다.

    고정 배수와 달리 매 시점 배수가 달라지지만, 실제 노출(=ex-ante TE 의 주 동인)이
    항상 목표값이 된다. 팩터 겹침 정도(netting)는 투자 판단이 아니라 부산물이므로
    그것이 포트 크기를 결정하지 않게 하는 것이 목적 (2026-08-19 채택).
    """
    if book_gross <= 0:
        logger.warning("book gross 가 0 이하 - 배수 1.0 로 폴백")
        return 1.0
    return target_gross / book_gross


def resolve_multiplier(as_of, path) -> float:
    """기준일에 유효한 배수 = effective_date <= as_of 중 가장 최근 값."""
    path = Path(path)
    if not path.exists():
        return 1.0
    hist = pd.read_csv(path, parse_dates=["effective_date"]).sort_values("effective_date")
    eligible = hist[hist["effective_date"] <= pd.Timestamp(as_of)]
    return float(eligible["multiplier"].iloc[-1]) if len(eligible) else 1.0


def apply_multiplier(df: pd.DataFrame, multiplier: float) -> pd.DataFrame:
    """종목 비중 컬럼에만 배수를 적용한다.

    factor_weight 는 팩터 배분(피벗 컬럼 키)이라 스케일하지 않는다 — 곱하면
    피벗 헤더가 바뀌고 팩터 비중의 의미(합=1)도 깨진다.
    """
    if multiplier == 1.0:
        return df
    for c in MULTIPLIER_COLS:
        if c in df.columns:
            df[c] = df[c] * multiplier
    return df


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
    # 빈 포트폴리오 가드: 한쪽(롱 또는 숏) 종목이 0개인 팩터(라벨이 한쪽만 존재)면
    # pivot 결과에 return_weight 컬럼이 없어 KeyError. 빈 결과를 반환하고 호출부
    # (_compute_factor_net_return)가 비어있지 않은 쪽만 합산한다.
    if portfolio_data_df.empty:
        empty = pd.DataFrame(columns=[factor_abbr], dtype=float)
        return empty, empty.copy(), empty.copy()

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
