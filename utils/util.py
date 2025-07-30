from __future__ import annotations

"""Download & factor utilities (rich progress + logging)

Features
--------
* **Rich** progress bar (stays on top of log lines)
* Structured logging with ``RichHandler`` (when run as script)
* Helper functions for pickling, ranking, quantile labelling, etc.

Revision 2025‑07‑25
-------------------
* **Return type annotations reinstated** for public APIs:
  * ``assign_factor`` → returns a 4‑tuple of ``pd.DataFrame`` **or** a 4‑tuple
    of ``None`` when the factor has insufficient history.
  * ``download`` → returns a tuple of three ``List[str]`` plus the ``data_list``.
* Added more explicit *Returns* section in docstrings.
* No other logic changes.
"""

import logging
import pickle
from pathlib import Path
from typing import Any, List, Tuple

import numpy as np
import pandas as pd
from rich.progress import track

from port.query_structure import GenerateQueryStructure

# ----------------------------------------------------------------------------
# Logging setup (only used when run as a script)
# ----------------------------------------------------------------------------
logger = logging.getLogger(__name__)

def _dump_pickle(obj: Any, path: Path, *, protocol: int = pickle.HIGHEST_PROTOCOL) -> None:
    """Save *obj* to *path* using pickle.

    Ensures parent directories exist before writing so callers can simply pass
    a target file path without worrying about the directory tree.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(pickle.dumps(obj, protocol=protocol))

# =============================================================================
# Numeric helper utilities
# =============================================================================

def _scale_rank(series: pd.Series) -> pd.Series:
    """Map 1‑*n* ranks to a 1‑99 scale (NaN if *n* ≤ 10)."""
    n = len(series)
    if n <= 10:
        return pd.Series(np.nan, index=series.index)
    return (series - 1) * (99 / (n - 1)) + 1


def _quantile_label(score: float | int | np.floating) -> str | float:
    """Return Q1‑Q5 label for *score* (1‑100); returns ``np.nan`` if out‑of‑range."""
    if not (1 <= score <= 100):
        return np.nan
    return f"Q{int((score - 1) // 20 + 1)}"  # 20‑point buckets


def _add_initial_zero(series: pd.DataFrame) -> pd.DataFrame:
    """Insert a zero one month prior to the first observation (baseline)."""
    series.loc[series.index[0] - pd.DateOffset(months=1)] = 0
    return series.sort_index()

# ----------------------------------------------------------------------------
# Core factor assignment
# ----------------------------------------------------------------------------

def _assign_factor(
        abbv: str,
        order: int,
        query: pd.DataFrame,
        meta: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame] | Tuple[None, None, None, None]:
    """Compute sector/quantile/spread returns for *abbv* factor.

    Parameters
    ----------
    abbv, order
        Factor abbreviation and ranking direction (1=ascending, 0/‑1=descending).
    query
        Full raw factor dataframe (includes *M_RETURN* rows).
    meta
        ``query`` joined to factor metadata (for style/order lookup).

    Returns
    -------
    Tuple[DataFrame, DataFrame, DataFrame, DataFrame] | tuple[None, None, None, None]
        ``sector_ret``   – average monthly returns by sector × quantile (Q1‑Q5)
        ``quantile_ret`` – overall market returns by quantile (Q1‑Q5)
        ``spread``       – long‑short (Q1‑Q5) monthly performance series
        ``merged``       – underlying stock‑level frame used for calculations

        If the factor has ≤100 data points, all four elements are ``None``.
    """

    # ------------------------------------------------------------------
    # 1. Collect and lag the factor series
    # ------------------------------------------------------------------
    fld = meta[meta["factorAbbreviation"] == abbv].dropna().reset_index(drop=True)
    if len(fld) <= 100:
        logger.warning("Skipping %s – insufficient history", abbv)
        return None, None, None, None

    lag_col = abbv  # use factor code as new column name
    fld[lag_col] = fld.groupby("gvkeyiid")["val"].shift(1)
    fld = fld.dropna(subset=[lag_col]).drop(columns=["val", "factorAbbreviation"])

    # ------------------------------------------------------------------
    # 2. Pull monthly market returns (M_RETURN)
    # ------------------------------------------------------------------
    m_ret = (
        query[query["factorAbbreviation"] == "M_RETURN"].reset_index(drop=True)
        .rename(columns={"val": "M_RETURN"})
        .drop(columns=["factorAbbreviation"])
    )

    # ------------------------------------------------------------------
    # 3. Merge factor + returns, filter bad sectors
    # ------------------------------------------------------------------
    merged = (
        fld.merge(
            m_ret,
            on=["gvkeyiid", "ticker", "isin", "ddt", "sec", "country"],
            how="inner",
        )
        .query("sec != 'Undefined'")
        .reset_index(drop=True)
    )

    # ------------------------------------------------------------------
    # 4. Within‑sector ranking, score, quantile bucket
    # ------------------------------------------------------------------
    merged["rank"] = (
        merged.groupby(["ddt", "sec"])[lag_col].rank(method="average", ascending=bool(order))
    )
    merged["score"] = merged.groupby(["ddt", "sec"])["rank"].transform(_scale_rank)
    merged["quantile"] = merged["score"].apply(_quantile_label)
    merged = merged.dropna(subset=["quantile"])

    # ------------------------------------------------------------------
    # 5. Sector & market quantile returns
    # ------------------------------------------------------------------
    sector_ret = (
        merged.groupby(["ddt", "sec", "quantile"])["M_RETURN"].mean().unstack(fill_value=0)
    ).groupby("sec").mean().T

    quantile_ret = merged.groupby(["ddt", "quantile"])["M_RETURN"].mean().unstack(fill_value=0)

    # ------------------------------------------------------------------
    # 6. Q1‑Q5 spread (long‑short)
    # ------------------------------------------------------------------
    spread = pd.DataFrame({abbv: quantile_ret.iloc[:, 0] - quantile_ret.iloc[:, -1]})
    spread = _add_initial_zero(spread)

    return sector_ret, quantile_ret, spread, merged

# =============================================================================
# Download driver – orchestrates query, processing, and persistence
# =============================================================================

def download(
    start_date: str,
    end_date: str,
    *,
    info_path: Path | str = "data/factor_info.csv",
    out_dir: Path | str | None = None,
) -> Tuple[List[str], List[str], List[str], List[Any]]:
    """Run full pipeline and write four pickle files to disk.

    Returns
    -------
    tuple
        (``abbr_list``, ``name_list``, ``style_list``, ``data_list``)
    """

    out_dir = Path(out_dir) if out_dir else Path(__file__).resolve().parent.parent / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Pickles → %s", out_dir)

    # 1️⃣ Fetch raw factors within date range
    query = (
        GenerateQueryStructure(start_date, end_date)
        .fetch_snp()
        .drop_duplicates(
            subset=["ddt", "gvkeyiid", "factorAbbreviation", "val"],
            keep="last",
            ignore_index=True,
        )
    )

    # 2️⃣ Join with metadata (order/style/name)
    info = pd.read_csv(info_path)
    meta = query.merge(info, on="factorAbbreviation", how="inner")

    abbrs, orders = info.factorAbbreviation.tolist(), info.factorOrder.tolist()

    # 3️⃣ Assign factors with rich progress bar
    data_list: List[Any] = []
    for abbr, order in track(zip(abbrs, orders), total=len(abbrs), description="Assigning factors"):
        data_list.append(_assign_factor(abbr, order, query, meta))

    # 4️⃣ Persist outputs
    _dump_pickle(abbrs, out_dir / "list_abbv.pkl")
    _dump_pickle(info.factorName.tolist(), out_dir / "list_name.pkl")
    _dump_pickle(info.styleName.tolist(), out_dir / "list_style.pkl")
    _dump_pickle(data_list, out_dir / "list_data.pkl")

    logger.info("Done – %d factors processed", len(abbrs))
    return abbrs, info.factorName.tolist(), info.styleName.tolist(), data_list
