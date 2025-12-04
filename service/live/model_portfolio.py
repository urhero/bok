# -*- coding: utf-8 -*-
"""
End-to-End Factor Pipeline (v4-complete)
=======================================
This pipeline cleans raw factor data, constructs monthly spread return
matrices, ranks factors by CAGR, optimises two‑factor mixes, and exports
CSV artefacts — **while preserving the column order specified in
`meta['factorAbbreviation']`.**

Key outputs
-----------
| File                          | Description                                                      |
|-------------------------------|------------------------------------------------------------------|
| `final_pivot_yymmdd.csv`      | Pivoted weight matrix with individual factor exposure            |
| `final_style_yymmdd.csv`      | Weight panel data per categorized factor style                   |
| `final_factor_yymmdd.csv`     | Weight panel data with individual factor exposure                |
| `final_mp_yymmdd.csv`         | Affordable model portfolio                                       |
"""
from __future__ import annotations

import logging
import pickle
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from rich.progress import track


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global paths
# ---------------------------------------------------------------------------
DATA_DIR = Path.cwd() / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# =============================================================================
# 1️⃣ Pickle Loader
# =============================================================================

def _load_pickles(dir_: Path = DATA_DIR) -> Tuple[List[str], List[str], List[str], List[Any]]:
    """Load four pickled lists produced by the *download* stage."""
    files = ["list_abbv.pkl", "list_name.pkl", "list_style.pkl", "list_data.pkl"]
    loaded: List[Any] = []
    for fn in files:
        p = dir_ / fn
        if not p.exists():
            logger.error("Pickle '%s' missing", fn)
            raise FileNotFoundError(fn)
        loaded.append(pickle.loads(p.read_bytes()))
    logger.info("Loaded %d factors from pickles", len(loaded[0]))
    return tuple(loaded)  # type: ignore[return-value]

# =============================================================================
# 2️⃣ Sector Filter + Relabelling
# =============================================================================

def _filter_grouped(
    list_abbrs: List[str],
    list_names: List[str],
    list_styles: List[str],
    list_data: List[Tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None]],
) -> Tuple[List[str], List[str], List[str], List[int], List[List[str]], List[pd.DataFrame]]:
    """Remove sectors with negative Q‑spread; recompute long/short labels."""

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
        tmp["spread"] = tmp["Q1"] - tmp["Q5"]
        to_drop = tmp.loc[tmp["spread"] < 0, "sec"].tolist()
        raw_clean = raw_df[~raw_df["sec"].isin(to_drop)].reset_index(drop=True)
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
# 3️⃣ Return Matrix · Ranking · Negative Correlation
# =============================================================================

def _ncorr(df: pd.DataFrame, min_obs: int = 20) -> pd.DataFrame:
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
    raw_df["wgt_rtn"] = 1 / raw_df["num"] * raw_df["label"]
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
    tvr_df = port_raw.pivot_table(index="ddt", columns="gvkeyiid", values="wgt_tvr")
    sgn_df = np.sign(wgt_df)

    r = rtn_df.sort_index()
    w = tvr_df.reindex(r.index)
    w0 = tvr_df.copy()
    is_rebal = w.notna().any(axis=1).fillna(False)
    block_id = is_rebal.cumsum().astype(int)
    cumG_blk = (1 + sgn_df * r).groupby(block_id).cumprod()

    denom = (w0 * cumG_blk).sum(axis=1)
    w_pre = (w0 * cumG_blk).div(denom, axis=0)

    wgt_df.iloc[0] = w0.loc[wgt_df.index[0]]
    rebal_in_r = r.index.intersection(tvr_df.index)
    turnover = 1 * (w.shift(-1).loc[rebal_in_r] - w_pre.loc[rebal_in_r]).abs().sum(axis=1)
    turnover = turnover.reindex(r.index).fillna(0)
    trading_friction = (cost_bps / 1e4) * turnover

    _gross = (wgt_df * r).sum(axis=1)
    _gross_df = _gross.to_frame().rename(columns={0: abbr_nms})

    _tf_df = trading_friction.to_frame().rename(columns={0: abbr_nms})
    _net_df = _gross_df - _tf_df

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

    return df_grs, df_net, df_trc


def _generate_meta(
    abbrs: List[str],
    names: List[str],
    styles: List[str],
    data: List[pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info("Building monthly return matrix")
    ret_df = _aggregate_returns(data, abbrs)[1]
    ret_df.loc[ret_df.index[0]] = 0.0
    ret_df = ret_df.sort_index()

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
# 4️⃣ Two‑Factor Mix Optimiser
# =============================================================================


def _get_wgt(
        factor_rets: pd.DataFrame,
        data_raw: pd.DataFrame,
        data_neg: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[pd.Series], str, float, str, float]:
    """
    Grid-search the optimal weight split for a main/sub factor pairs.

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
# 5️⃣ Assemble Style Portfolios
# =============================================================================

def assemble_top_style_portfolios(
    factor_rets: pd.DataFrame,
    meta: pd.DataFrame,
    neg_corr: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Select #1 factor per style and generate its optimal mix series."""

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
# 6️⃣ Simulate Factor Exposures
# =============================================================================

def random_style_capped_sim(
    rtn_df: pd.DataFrame,
    style_list: List[str],
    num_sims: int = 1_000_000,
    style_cap: float = 0.25,
    tol: float = 1e-12,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Monte-Carlo search for the best style-capped portfolio.

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
    """Run the full ETL → optimisation → export process."""
    logger.info("Report generation started for period: %s to %s", start_date, end_date)

    # 1. Load pickles and apply sector filter
    abbrs, names, styles, raw = _load_pickles()
    kept_abbr, kept_name, kept_style, _, _, cleaned_raw = _filter_grouped(abbrs, names, styles, raw)

    # 2. Build return matrix, negative correlation matrix and meta ranking table
    rtns, norr, meta = _generate_meta(kept_abbr, kept_name, kept_style, cleaned_raw)

    # 3. Generate weight grids only for the top factor in each style
    top_meta = meta.groupby("styleName", as_index=False).first()
    grids = []
    for _, row in top_meta.iterrows():
        grid, *_ = _get_wgt(rtns, row.to_frame().T.reset_index(drop=True), norr)
        grid["styleName"] = row["styleName"]
        grids.append(grid)
    mix_grid = pd.concat(grids, ignore_index=True)

    # Subset return matrix to selected factors
    # 5. Best sub_factor for each main_factor  ── add style names
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

    # Subset return matrix to selected factors
    cols_to_keep = pd.unique(best_sub[["main_factor", "sub_factor"]].to_numpy().ravel())
    ret_subset = rtns[cols_to_keep]

    # 7. Build factor_list & style_list (aligned order)
    factor_list = pd.unique(best_sub[["main_factor", "sub_factor"]].to_numpy().ravel()).tolist()
    style_list = [style_map[f] for f in factor_list]

    res = random_style_capped_sim(ret_subset, style_list)

    # ------------------------------------------------------------------
    # 8. Build per-factor weight tables (date × id × weight)
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
    # 9. Aggregate across factors  (Σ weights per date × security)
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