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
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union
from config import PARAM

import numpy as np
import pandas as pd
import time
from rich.progress import track


# ---------------------------------------------------------------------------
# 로깅 설정
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)



# =============================================================================
# 범용 헬퍼 함수
# =============================================================================




# =============================================================================
# 수치 계산 헬퍼 유틸리티
# =============================================================================

def _rank_to_percentile(series: pd.Series) -> pd.Series:
    """1~n 순위를 0~100 스케일로 변환 (n ≤ 10이면 NaN 반환)"""
    n = len(series)
    if n <= 10:
        return pd.Series(np.nan, index=series.index)
    return (series - 1) * (100 / (n - 1))

def _n_quantile_label(score: float | int | np.floating, n: int = 5) -> Union[str, float]:
    """점수(1~100)를 n분위 라벨(Q1~Qn)로 변환"""
    if not (1 <= score <= 100) or n <= 0:  # 100으로 정확히 나누어지는 분위수로 하려면  or 100 % n != 0 추가
        return np.nan 
    bucket_size = 100 / n
    return f"Q{int((score - 1) // bucket_size + 1)}"


def _add_initial_zero(series: pd.DataFrame) -> pd.DataFrame:
    """첫 관측값 한 달 전에 0을 삽입 (기준선 설정)"""
    series.loc[series.index[0] - pd.DateOffset(months=1)] = 0
    return series.sort_index()

# ----------------------------------------------------------------------------
# 핵심 팩터 할당 로직
# ----------------------------------------------------------------------------

def _assign_factor(
        abbv: str,
        order: int,
        fld: pd.DataFrame,
        m_ret: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame] | Tuple[None, None, None, None]:
    """특정 팩터에 대한 섹터/분위수/스프레드 수익률 계산

    Parameters
    ----------
    abbv, order
        팩터 약어 및 순위 방향 (1=오름차순, 0/-1=내림차순)
    fld
        해당 팩터의 데이터프레임 (이미 필터링됨)
    m_ret
        시장 수익률 데이터프레임 (이미 추출됨)

    Returns
    -------
    Tuple[DataFrame, DataFrame, DataFrame, DataFrame] | tuple[None, None, None, None]
        ``sector_ret``   – 섹터 × 분위수(Q1‑Q5)별 평균 월간 수익률
        ``quantile_ret`` – 분위수(Q1‑Q5)별 전체 시장 수익률
        ``spread``       – 롱‑숏(Q1‑Q5) 월간 성과 시계열
        ``merged``       – 계산에 사용된 기본 종목 수준 데이터프레임

        팩터의 데이터 포인트가 ≤100개인 경우, 4개 요소 모두 ``None``
    """

    # ------------------------------------------------------------------
    # 1. 팩터 시계열 수집 및 래그 적용
    # ------------------------------------------------------------------
    # fld는 이미 필터링되어 전달됨
    fld = fld.dropna().reset_index(drop=True)

    # 히스토리가 충분하지 않으면 스킵
    if len(fld['ddt'].unique()) <= 2:
        logger.warning("Skipping %s – insufficient history", abbv)
        return None, None, None, None

    # 각 종목별로 1개월 래그 적용 (전월 팩터 값을 당월에 사용)
    fld[abbv] = fld.groupby("gvkeyiid")["val"].shift(1)
    fld = fld.dropna(subset=[abbv]).drop(columns=["val", "factorAbbreviation"])

    # ------------------------------------------------------------------
    # 2. 월간 시장 수익률(M_RETURN) 추출
    # ------------------------------------------------------------------
    # m_ret는 이미 추출되어 전달됨

    # ------------------------------------------------------------------
    # 3. 팩터 + 수익률 병합, 잘못된 섹터 필터링
    # ------------------------------------------------------------------
    merged = (
        fld.merge(
            m_ret,
            on=["gvkeyiid", "ticker", "isin", "ddt", "sec", "country"],
            how="inner",
        )
        .query("sec != 'Undefined'")  # 정의되지 않은 섹터 제거
        .reset_index(drop=True)
    )

    # ------------------------------------------------------------------
    # 4. 섹터 내 순위 매기기, 점수화, 분위수 버킷 할당
    # ------------------------------------------------------------------

    # 날짜 및 섹터별로 팩터 값에 대한 순위 계산
    merged["rank"] = (
        merged.groupby(["ddt", "sec"])[abbv].rank(method="average", ascending=bool(order))
    )

    # 순위를 0~100 점수로 변환
    merged["score"] = merged.groupby(["ddt", "sec"])["rank"].transform(_rank_to_percentile)
    # 점수를 Q1~Q5 분위수 라벨로 변환
    merged["quantile"] = merged["score"].apply(_n_quantile_label, n=5)
    merged = merged.dropna(subset=["quantile"])

    # ------------------------------------------------------------------
    # 5. 섹터 및 시장 분위수 수익률 계산
    # ------------------------------------------------------------------

    # 섹터별 분위수 평균 수익률 계산 (?기하 수익률로 고쳐야함, 하드코딩)
    sector_ret = (
        merged.groupby(["ddt", "sec", "quantile"])["M_RETURN"].mean().unstack(fill_value=0)
    ).groupby("sec").mean().T

    # 전체 시장의 분위수별 평균 수익률 계산 (모든 섹터 포함?) (?기하 수익률로 고쳐야함, 하드코딩)
    quantile_ret = merged.groupby(["ddt", "quantile"])["M_RETURN"].mean().unstack(fill_value=0)

    # ------------------------------------------------------------------
    # 6. Q1‑Q5 스프레드 계산 (롱‑숏 전략)
    # ------------------------------------------------------------------
    # Q1(최고) - Q5(최저) 수익률 차이 계산
    spread = pd.DataFrame({abbv: quantile_ret.iloc[:, 0] - quantile_ret.iloc[:, -1]})
    spread = _add_initial_zero(spread)

    return sector_ret, quantile_ret, spread, merged



# ---------------------------------------------------------------------------
# 전역 경로
# ---------------------------------------------------------------------------
DATA_DIR = Path.cwd() / "data" 
DATA_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# 2️⃣ 섹터 필터링 + 재라벨링
# =============================================================================

def _filter_grouped(
    list_abbrs: List[str],
    list_names: List[str],
    list_styles: List[str],
    list_data: List[Tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None]],
) -> Tuple[List[str], List[str], List[str], List[int], List[List[str]], List[pd.DataFrame]]:
    """음의 Q-스프레드를 가진 섹터 제거; 롱/숏 라벨 재계산"""

    kept_abbr, kept_name, kept_style, kept_idx = [], [], [], []
    dropped_sec: List[List[str]] = []
    new_raw: List[pd.DataFrame] = []

    for idx, (sec_df, _, _, raw_df) in track(
        enumerate(list_data), description="Filtering sectors", total=len(list_data)
    ):
        if sec_df is None or raw_df is None:
            logger.debug("Factor %d skipped – no data", idx)
            continue
        tmp = sec_df.T.reset_index()
        tmp["spread"] = tmp["Q1"] - tmp["Q5"]  # Q1-Q5 스프레드 계산
        to_drop = tmp.loc[tmp["spread"] < 0, "sec"].tolist()  # 음의 스프레드 섹터 식별
        raw_clean = raw_df[~raw_df["sec"].isin(to_drop)].reset_index(drop=True)  # 해당 섹터 제거
        if raw_clean.empty:
            logger.debug("Factor %d discarded – all sectors dropped", idx)
            continue

        q_ret = raw_clean.groupby(["ddt", "quantile"])["M_RETURN"].mean().unstack(fill_value=0)
        q_mean = q_ret.mean(axis=0).to_frame("mean")
        thresh = abs(q_mean.loc["Q1", "mean"] - q_mean.loc["Q5", "mean"]) * 0.10
        # >= <=
        q_mean["long"] = (q_mean["mean"] > q_mean.loc["Q1", "mean"] - thresh).astype(int).cumprod()
        q_mean["short"] = (q_mean["mean"] < q_mean.loc["Q5", "mean"] + thresh).astype(int) * -1
        q_mean["short"] = q_mean["short"].abs()[::-1].cumprod()[::-1] * -1
        q_mean["label"] = q_mean["long"] + q_mean["short"]
        merged = raw_clean.merge(q_mean.reset_index()[["quantile", "label"]], on="quantile")

        kept_abbr.append(list_abbrs[idx]); kept_name.append(list_names[idx]); kept_style.append(list_styles[idx]); kept_idx.append(idx)
        dropped_sec.append(to_drop); new_raw.append(merged)

    logger.info("Sector filter retained %d / %d factors", len(kept_idx), len(list_abbrs))
    return kept_abbr, kept_name, kept_style, kept_idx, dropped_sec, new_raw

# =============================================================================
# 3️⃣ 수익률 행렬 · 순위 · 하락 상관관계
# =============================================================================

def _ncorr(df: pd.DataFrame, min_obs: int = 20) -> pd.DataFrame:
    """음의 수익률 기간 동안의 상관관계 계산(Downside Correlation 에 가깝지만 기준 자산 수익률이 음수인 모든 기간을 포함)"""
    out = pd.DataFrame(index=df.columns, columns=df.columns, dtype=float)
    for col in df.columns:
        mask = df[col] < 0
        out.loc[col] = df.loc[mask].corr()[col] if mask.sum() >= min_obs else np.nan
    return out


def _ls_portfolio(
        data_raw: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    raw_df = data_raw[data_raw["ddt"] >= "2017-12-31"].reset_index(drop=True).copy()
    raw_df["signal"] = raw_df["label"].map({1: "L", 0: "N", -1: "S"})
    raw_df["num"] = raw_df.groupby(["ddt", "signal"])["signal"].transform("count")
    # wgt_rtn은 포트폴리오 비중임, 수익률 계산용 #?
    raw_df["wgt_rtn"] = 1 / raw_df["num"] * raw_df["label"]
    # tvr_df 는 턴오버 계산용 wgt임. 실제 턴오버는 trading_friction #?
    raw_df["wgt_tvr"] = abs(raw_df["wgt_rtn"])
    raw_df_l = raw_df[raw_df["signal"] == "L"].reset_index(drop=True)
    raw_df_s = raw_df[raw_df["signal"] == "S"].reset_index(drop=True)
    return raw_df_l, raw_df_s


def _vectorized_bt(
        port_raw: pd.DataFrame,
        abbr_nms: str,
        cost_bps: float = 30.0
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    wgt_df = port_raw.pivot_table(index="ddt", columns="gvkeyiid", values="wgt_rtn")
    rtn_df = port_raw.pivot_table(index="ddt", columns="gvkeyiid", values="M_RETURN")
    rtn_df.iloc[0] = 0
    # tvr_df 는 턴오버 계산용 wgt임. 실제 턴오버는 trading_friction #?
    tvr_df = port_raw.pivot_table(index="ddt", columns="gvkeyiid", values="wgt_tvr")
    sgn_df = np.sign(wgt_df)

    r = rtn_df.sort_index()
    w = tvr_df.reindex(r.index)
    w0 = tvr_df.copy()
    is_rebal = w.notna().any(axis=1).fillna(False)  #? # 각 날짜별 NA가 아닌게 하나라도 있으면 True
    block_id = is_rebal.cumsum().astype(int)  #? 리밸런싱 블럭 1,2,3,....
    cumG_blk = (1 + sgn_df * r).groupby(block_id).cumprod() # 블럭내에서 누적 곱

    denom = (w0 * cumG_blk).sum(axis=1)  #각 날짜 비중 합
    w_pre = (w0 * cumG_blk).div(denom, axis=0)  # 각 날짜 비중 100%로 조정

    wgt_df.iloc[0] = w0.loc[wgt_df.index[0]] # 첫날 비중
    rebal_in_r = r.index.intersection(tvr_df.index)  # 리밸런싱 웨이트와 수익률 날짜(인덱스) 교집합으로 리밸런싱 날짜 선택
    turnover = 1 * (w.shift(-1).loc[rebal_in_r] - w_pre.loc[rebal_in_r]).abs().sum(axis=1)  # 리밸런싱 날짜의 웨이트 차이
    turnover = turnover.reindex(r.index).fillna(0)  # 리밸런싱 날짜 외의 날짜는 0으로 채움
    trading_friction = (cost_bps / 1e4) * turnover  # 거래비용

    _gross = (wgt_df * r).sum(axis=1)  # 날짜별 수익률 (이미 시프트 되어 있음)
    _gross_df = _gross.to_frame().rename(columns={0: abbr_nms})  # 날짜별 수익률(거래비용 차감전) 

    _tf_df = trading_friction.to_frame().rename(columns={0: abbr_nms})  # 날짜별 거래비용, 시리즈를 데이터프레임으로 변환
    _net_df = _gross_df - _tf_df  # 날짜별 수익률(거래비용 차감전) - 거래비용

    return _gross_df, _net_df, _tf_df


def _aggregate_returns(
        data_raw: List[pd.DataFrame],
        abbr_nms: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    list_grs, list_net, list_trc = [], [], []
    for list_raw, list_abbr in zip(data_raw, abbr_nms):

        dfl, dfs = _ls_portfolio(list_raw)
        res_grs_l, res_net_l, res_trc_l = _vectorized_bt(dfl, list_abbr)
        res_grs_s, res_net_s, res_trc_s = _vectorized_bt(dfs, list_abbr)
        list_grs.append(res_grs_l + res_grs_s)
        list_net.append(res_net_l + res_net_s)
        list_trc.append(res_trc_l + res_trc_s)

    df_grs = pd.concat(list_grs, axis=1).dropna(axis=1)
    df_net = pd.concat(list_net, axis=1).dropna(axis=1)
    df_trc = pd.concat(list_trc, axis=1).dropna(axis=1)

    return df_grs, df_net, df_trc  # gross returns, net returns, trading costs?


def _generate_meta(
    abbrs: List[str],
    names: List[str],
    styles: List[str],
    data: List[pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info("Building monthly return matrix")
    ret_df = _aggregate_returns(data, abbrs)[1]
    ret_df.loc[ret_df.index[0]] = 0.0
    ret_df = ret_df.sort_index()  # 날짜 오름차순, 혹시나 싶어서 함 #?

    valid = ret_df.columns[(ret_df == 0).sum() <= 10]
    ret_df = ret_df[valid]

    meta = (
        pd.DataFrame({"factorAbbreviation": abbrs, "factorName": names, "styleName": styles})
        .set_index("factorAbbreviation")
        .loc[valid]
        .reset_index()
    )

    months = len(ret_df) - 1
    meta["cagr"] = ((1 + ret_df).cumprod().iloc[-1] ** (12 / months) - 1).values
    meta["rank_style"] = meta.groupby("styleName")["cagr"].rank(ascending=False)
    meta["rank_total"] = meta["cagr"].rank(ascending=False)
    meta.to_csv("meta_data.csv")
    meta = meta.sort_values("rank_total").reset_index(drop=True).rename(columns={"index": "factorAbbreviation"})[:50]

    order = meta["factorAbbreviation"].tolist()
    ret_df = ret_df[order]
    negative_corr = _ncorr(ret_df).loc[order, order]

    logger.info("Return matrix built (%d factors)", len(order))
    return ret_df, negative_corr, meta

# =============================================================================
# 4️⃣ 2-팩터 믹스 최적화
# =============================================================================


def _get_wgt(
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

def assemble_top_style_portfolios(
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

        df_mix, series_list, *_ = _get_wgt(
            factor_rets, row.to_frame().T.reset_index(drop=True), neg_corr
        )
        best_idx = df_mix.nsmallest(1, "rank_total").index[0]
        mixes[tag] = series_list[best_idx].rename(tag)

    style_df = pd.concat(mixes.values(), axis=1)
    style_neg_corr = _ncorr(style_df)
    logger.info("Built %d style portfolios", style_df.shape[1])
    return style_df, style_neg_corr



# =============================================================================
# 6️⃣ 팩터 노출도 시뮬레이션
# =============================================================================

def random_style_capped_sim(
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
    
    weights_tbl = pd.DataFrame({
        "factor": np.array(['SalesAcc', '6MTTMSalesMom', 'PM6M', '52WSlope', '90DCV', 
            'CashEV', 'RevMagFY1C', 'SalesToEPSChg', 'Rev3MFY1C', 'TobinQ']),
        "raw_weight": np.array([0.199298652556654,0.00842206236153488,0.196025173956866,0.0326737859629076,0.174696911741135,
            0.148243451062375,0.10775464398236,0.0577835874187986,0.0524125911883854,0.022689139768980]),
        "styleName": np.array(['Historical Growth', 'Historical Growth', 'Price Momentum', 'Price Momentum', 'Volatility', 
            'Valuation', 'Analyst Expectations', 'Earnings Quality', 'Analyst Expectations', 'Capital Efficiency']),
        "fitted_weight": np.array([0.199298652556654,0.00842206236153488,0.196025173956866,0.0326737859629076,0.174696911741135,
            0.148243451062375,0.10775464398236,0.0577835874187986,0.0524125911883854,0.022689139768980]),
    })

    # weights_tbl = pd.DataFrame({
    #     "factor": factors,
    #     "raw_weight": raw_mat[:, best_idx],
    #     "styleName": styles,
    #     "fitted_weight": fitted_mat[:, best_idx],
    # })

    # weights_tbl = (
    #     weights_tbl[weights_tbl["raw_weight"] > 0]
    #     .sort_values("raw_weight", ascending=False)
    #     .reset_index(drop=True)
    # )

    return best_stats, weights_tbl

def mp(start_date, end_date) -> None:

    # parquet 파일 로드하기
    t0 = time.time()
    parquet_path = DATA_DIR / f"{PARAM['benchmark']}_{start_date}_{end_date}.parquet"
    query = pd.read_parquet(parquet_path)
    logger.info(f"Query loaded from {parquet_path} in {time.time() - t0:.2f}s")

    # 2️⃣ 메타데이터(순서/스타일/이름)와 조인
    t1 = time.time()
    info_path = DATA_DIR / "factor_info.csv"
    info = pd.read_csv(info_path)
    meta = query.merge(info, on="factorAbbreviation", how="inner")

    abbrs, orders = info.factorAbbreviation.tolist(), info.factorOrder.tolist()

    # 3️⃣ Rich 진행바와 함께 팩터 할당
    # 최적화: M_RETURN 미리 추출
    m_ret = (
        query[query["factorAbbreviation"] == "M_RETURN"].reset_index(drop=True)
        .rename(columns={"val": "M_RETURN"})
        .drop(columns=["factorAbbreviation"])
    )

    # 최적화: meta를 미리 그룹화
    grouped_meta = meta.groupby("factorAbbreviation")

    data_list: List[Any] = []
    for abbr, order in track(zip(abbrs, orders), total=len(abbrs), description="Assigning factors"):
        # 그룹이 존재하면 가져오고, 없으면 빈 DataFrame 전달
        if abbr in grouped_meta.groups:
            fld = grouped_meta.get_group(abbr)
        else:
            fld = pd.DataFrame(columns=meta.columns)
        
        data_list.append(_assign_factor(abbr, order, fld, m_ret))
    logger.info(f"Factors assigned in {time.time() - t1:.2f}s")

    """Run the full ETL → optimisation → export process."""
    logger.info("Report generation started for period: %s to %s", start_date, end_date)

    # 1. 피클 로드 및 섹터 필터 적용
    abbrs, names, styles, raw = abbrs, info.factorName.tolist(), info.styleName.tolist(), data_list
    kept_abbr, kept_name, kept_style, _, _, cleaned_raw = _filter_grouped(abbrs, names, styles, raw)

    # 2. 수익률 행렬, 음의 상관관계 행렬, 메타 순위 테이블 생성
    rtns, norr, meta = _generate_meta(kept_abbr, kept_name, kept_style, cleaned_raw)

    # 3. 각 스타일의 최상위 팩터(팩터들?)에 대해서만 가중치 그리드 생성
    top_meta = meta.groupby("styleName", as_index=False).first()
    grids = []
    for _, row in top_meta.iterrows():
        grid, *_ = _get_wgt(rtns, row.to_frame().T.reset_index(drop=True), norr)
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
    style_map = meta.set_index("factorAbbreviation")["styleName"]
    best_sub["main_style"] = best_sub["main_factor"].map(style_map)
    best_sub["sub_style"] = best_sub["sub_factor"].map(style_map)
    best_sub = best_sub[["main_factor", "main_style", "sub_factor", "sub_style"]]

    # Save if needed
    # best_sub.to_csv(DATA_DIR / "best_sub_factor.csv", index=False)

    # 6. 메인 팩터와 보조 팩터로 수익률 행렬 합집합 생성
    cols_to_keep = pd.unique(best_sub[["main_factor", "sub_factor"]].to_numpy().ravel())
    ret_subset = rtns[cols_to_keep]

    # 7. factor_list 및 style_list 구성 (정렬된 순서)
    factor_list = pd.unique(best_sub[["main_factor", "sub_factor"]].to_numpy().ravel()).tolist()
    style_list = [style_map[f] for f in factor_list]

    res = random_style_capped_sim(ret_subset, style_list)

    # ------------------------------------------------------------------
    # 8. 팩터별 가중치 테이블 구성 (date × id × weight)
    # ------------------------------------------------------------------
    weight_frames = []
    for _, row in res[1].iterrows():
        fac = row['factor']
        w = row['fitted_weight']
        s = row['styleName']

        j = kept_abbr.index(fac)
        df = cleaned_raw[j][['ddt', 'ticker', 'isin', 'gvkeyiid', 'label']].copy()
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
    agg_w['count'] =(
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

    agg_w.to_csv(DATA_DIR / f"aggregated_weights_{end_date}_test.csv")
    final_weights.to_csv(DATA_DIR / f"total_aggregated_weights_{end_date}_test.csv")

    final_style_weight.to_csv(DATA_DIR / f"total_aggregated_weights_style_{end_date}_test.csv")

    final_weights.loc[final_weights['style'] == 'MP', 'factor_weight'] = 1
    final_weights = final_weights.replace(0, np.nan)

    pivoted_final = final_weights.pivot_table(index=['ddt', 'ticker', 'isin', 'gvkeyiid'],
                                              columns=['style', 'factor_weight', 'factor'], values='weight',
                                              aggfunc='sum').reset_index()

    sample_df = pd.DataFrame({"factor": pivoted_final.columns.get_level_values(2).tolist()[4:]})
    sum_df = pd.merge(res[1], sample_df, on='factor', how='inner')

    final_weights.loc[final_weights['style'] == 'MP', 'factor_weight'] = sum_df['fitted_weight'].sum(axis=0)
    final_weights = final_weights.replace(0, np.nan)

    pivoted_final = final_weights.pivot_table(index=['ddt', 'ticker', 'isin', 'gvkeyiid'],
                                                    columns=['style', 'factor_weight', 'factor'], values='weight',
                                                    aggfunc='sum').reset_index()

    cols = pivoted_final.columns
    mp_mask = cols.get_level_values('style') == 'MP'

    new_order = cols[~mp_mask].tolist() + cols[mp_mask].tolist()
    pivoted_final = pivoted_final.loc[:, new_order]

    pivoted_final.to_csv(DATA_DIR / f"pivoted_total_agg_wgt_{end_date}.csv")
    logger.info("Pipeline completed ✓ — files saved in %s", DATA_DIR)