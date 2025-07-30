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
| File                  | Description                                                      |
|-----------------------|------------------------------------------------------------------|
| `factor_rets.csv`     | Monthly spread matrix in meta order                              |
| `neg_corr.csv`        | Negative‑return correlation matrix (meta order)                  |
| `style_portfolios.csv`| Best‑mix return series for each style (`ane`, `mom`, …)          |
| `style_neg_corr.csv`  | Negative‑return correlation between style portfolios             |
| `mix_grid.csv`        | 5 × 101 weight grid per style – includes `main_factor` & `sub_factor` |
"""
from __future__ import annotations

import logging
import pickle
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from rich.progress import track

__all__ = [
    "get_port_wgt",
    "assemble_top_style_portfolios",
]

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Global paths
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
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
        thresh = (q_mean.loc["Q1", "mean"] - q_mean.loc["Q5", "mean"]) * 0.10
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

def _compute_label(df_raw: pd.DataFrame, abbr: str) -> pd.Series:
    spread = df_raw.groupby(["ddt", "label"])["M_RETURN"].mean().unstack(fill_value=0)
    return (spread.iloc[:, -1] - spread.iloc[:, 0]).rename(abbr)


def _corr_when_negative(df: pd.DataFrame, min_obs: int = 20) -> pd.DataFrame:
    out = pd.DataFrame(index=df.columns, columns=df.columns, dtype=float)
    for col in df.columns:
        mask = df[col] < 0
        out.loc[col] = df.loc[mask].corr()[col] if mask.sum() >= min_obs else np.nan
    return out


def _build_returns(
    abbrs: List[str],
    names: List[str],
    styles: List[str],
    data: List[pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info("Building monthly return matrix")
    ret_df = pd.concat({_a: _compute_label(r, _a) for _a, r in zip(abbrs, data)}.values(), axis=1).fillna(0)
    ret_df.loc[ret_df.index[0] - pd.DateOffset(months=1)] = 0.0
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
    meta = meta.sort_values("rank_total").reset_index(drop=True).rename(columns={'index': 'factorAbbreviation'})[:50]

    order = meta["factorAbbreviation"].tolist()
    ret_df = ret_df[order]
    neg_corr = _corr_when_negative(ret_df).loc[order, order]

    logger.info("Return matrix built (%d factors)", len(order))
    return ret_df, neg_corr, meta

# =============================================================================
# 4️⃣ Two‑Factor Mix Optimiser
# =============================================================================

def _max_dd(cum: np.ndarray) -> np.ndarray:
    return (cum / np.maximum.accumulate(cum, axis=0) - 1).min(axis=0)


def get_port_wgt(
    factor_rets: pd.DataFrame,
    data_raw: pd.DataFrame,
    data_neg: pd.DataFrame,
    *_,
) -> Tuple[pd.DataFrame, List[pd.Series], str, float, str, float]:
    """Grid-search the optimal weight split for a main/sub factor pair.

    Returns
    -------
    df_mix : DataFrame
        Grid of weight pairs with performance metrics and rankings.
    ports  : list[Series]
        List of mix return series (one per grid column, order aligned with
        `df_mix`).
    main_factor, main_w, sub_factor, sub_w : str | float
        Identifiers and optimal weights.
    """

    # ------------------------------------------------------------------
    # 1. Build candidate list (five sub-factors with best combined rank)
    # ------------------------------------------------------------------
    negative_corr = (
        data_neg.loc[data_raw["factorAbbreviation"], :]
        .T.reset_index()
        .reset_index()
    )
    negative_corr.iloc[:, 0] += 1  # CAGR rank starts at 1
    negative_corr.columns = ["rank_cagr", "factorAbbreviation", negative_corr.columns[-1]]
    negative_corr["rank_negative_corr"] = negative_corr[negative_corr.columns[-1]].rank()
    negative_corr["rank_avg"] = negative_corr["rank_cagr"] * 0.7 + negative_corr["rank_negative_corr"] * 0.3
    negative_corr = negative_corr.nsmallest(5, "rank_avg")

    # ------------------------------------------------------------------
    # 2. Prepare weight grid & common variables
    # ------------------------------------------------------------------
    w_grid = np.linspace(0.0, 1.0, 101)
    w_inv = 1.0 - w_grid
    ann = 12 / factor_rets.shape[0]  # monthly → annual exponent
    main = data_raw["factorAbbreviation"].iat[0]

    frames: List[pd.DataFrame] = []
    mix_series: List[pd.Series] = []

    # ------------------------------------------------------------------
    # 3. Iterate over candidate sub-factors with a progress bar
    # ------------------------------------------------------------------
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
            "sub_factor": sub,
        })
        frames.append(df)

        # Store each mix return column as Series (aligned with df rows)
        mix_series.extend(
            pd.Series(mix_ret[:, i], index=port.index) for i in range(mix_ret.shape[1])
        )

        logger.info("Completed main=%s ↔ sub=%s", main, sub)

    # ------------------------------------------------------------------
    # 4. Concatenate grid & rank
    # ------------------------------------------------------------------
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

        df_mix, series_list, *_ = get_port_wgt(
            factor_rets, row.to_frame().T.reset_index(drop=True), neg_corr
        )
        best_idx = df_mix.nsmallest(1, "rank_total").index[0]
        mixes[tag] = series_list[best_idx].rename(tag)

    style_df = pd.concat(mixes.values(), axis=1)
    style_neg_corr = _corr_when_negative(style_df)
    logger.info("Built %d style portfolios", style_df.shape[1])
    return style_df, style_neg_corr


# =============================================================================
# 6️⃣ Simulate Factor Exposures
# =============================================================================

def random_style_capped_sim(
    rtn_df: pd.DataFrame,
    style_list: List[str],
    num_sims: int = 10_000_000,
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

    return best_stats, weights_tbl


def report(start_date, end_date) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run the full ETL → optimisation → export process."""
    logger.info("Report generation started for period: %s to %s", start_date, end_date)

    # 1. Load pickles and apply sector filter
    abbrs, names, styles, raw = _load_pickles()
    kept_abbr, kept_name, kept_style, _, _, cleaned_raw = _filter_grouped(abbrs, names, styles, raw)

    # 2. Build return matrix, neg-corr matrix, and meta ranking table
    factor_rets, neg_corr, meta = _build_returns(kept_abbr, kept_name, kept_style, cleaned_raw)

    # 3. Create style-level mixed portfolios
    style_df, style_neg_corr = assemble_top_style_portfolios(factor_rets, meta, neg_corr)

    # 4. Generate weight grids only for the top factor in each style
    top_meta = meta.groupby("styleName", as_index=False).first()
    grids = []
    for _, row in top_meta.iterrows():
        grid, *_ = get_port_wgt(factor_rets, row.to_frame().T.reset_index(drop=True), neg_corr)
        grid["styleName"] = row["styleName"]
        grids.append(grid)
    mix_grid = pd.concat(grids, ignore_index=True)

    # 5. Save outputs
    factor_rets.to_csv(DATA_DIR / "factor_rets.csv")
    neg_corr.to_csv(DATA_DIR / "neg_corr.csv")
    style_df.to_csv(DATA_DIR / "style_portfolios.csv")
    style_neg_corr.to_csv(DATA_DIR / "style_neg_corr.csv")
    mix_grid.to_csv(DATA_DIR / "mix_grid.csv", index=False)

    # Subset return matrix to selected factors
    # 6. Best sub_factor for each main_factor  ── add style names
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
    best_sub.to_csv(DATA_DIR / "best_sub_factor.csv", index=False)

    # Subset return matrix to selected factors
    cols_to_keep = pd.unique(best_sub[["main_factor", "sub_factor"]].to_numpy().ravel())
    ret_subset = factor_rets[cols_to_keep]

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

        j = kept_abbr.index(fac)
        df = cleaned_raw[j][['ddt', 'ticker', 'isin', 'gvkeyiid', 'label']].copy()
        df['weight'] = df['label'] * w / df.groupby(['ddt', 'label'])['label'].transform('count')
        weight_frames.append(df[['ddt', 'ticker', 'isin', 'gvkeyiid', 'weight']])

        # ------------------------------------------------------------------
        # 9. Aggregate across factors  (Σ weights per date × security)
        # ------------------------------------------------------------------
    agg_w = (
        pd.concat(weight_frames, ignore_index=True)
        .groupby(["ddt", "ticker", "isin", "gvkeyiid"], as_index=False)["weight"]
        .sum()
    )
    # ▶︎ zero-pad tickers to 6 chars
    agg_w["ticker"] = agg_w["ticker"].astype(str).str.zfill(6).add(" CH Equity")
    agg_w = agg_w[agg_w['ddt'] == end_date].reset_index(drop=True)

    agg_w.to_csv(DATA_DIR / f"aggregated_weights_{end_date}.csv")

    logger.info("Pipeline completed ✓ — files saved in %s", DATA_DIR)
    return factor_rets, meta
