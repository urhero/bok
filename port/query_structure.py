from __future__ import annotations

"""Lightweight wrapper around an MS SQL factor table.

* Adds rich docstrings, logging, and explicit type annotations.
* Keeps external interface identical: ``GenerateQueryStructure.fetch_snp``
  returns a pandas ``DataFrame`` with a new ``factorAbbreviation`` column.

Usage
-----
>>> gqs = GenerateQueryStructure("2024-01-31", "2024-12-31")
>>> df = gqs.fetch_snp()
"""

import logging
import re
from typing import Any, Dict

import pandas as pd
import sqlalchemy as sql

import config  # expects PARAM dict

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------------

def _extract_parentheses(text: str) -> str:
    """Return substring inside the first pair of parentheses, else *text*."""
    match = re.search(r"\((.*?)\)", text)
    return match.group(1) if match else text


def _assign_fld(frame: pd.DataFrame) -> pd.DataFrame:
    """Create ``factorAbbreviation`` column from ``fld`` then drop ``fld``."""
    frame = frame.copy()
    frame["factorAbbreviation"] = frame["fld"].apply(_extract_parentheses)
    return frame.drop(columns=["fld"])

# ----------------------------------------------------------------------------
# Core class
# ----------------------------------------------------------------------------

class GenerateQueryStructure:
    """Fetch raw factor data from SQL Server between *start* and *end* dates."""

    _param: Dict[str, Any] = config.PARAM  # centralised DB credentials/settings

    def __init__(self, start_date: str, end_date: str) -> None:
        self.start_date = start_date
        self.end_date = end_date

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------
    def fetch_snp(self) -> pd.DataFrame:
        """Return slice of factor universe as a tidy ``DataFrame``.

        Returns
        -------
        pd.DataFrame
            All columns from SQL table, plus ``factorAbbreviation`` (parsed
            from ``fld``).
        """
        arg = self._param

        logger.info("Fetching factors %s → %s (universe=%s)", self.start_date, self.end_date, arg["universe"])

        # Build SQLAlchemy connection URL (ODBC driver)
        conn_url = sql.engine.URL.create(
            "mssql+pyodbc",
            username=arg["user_name"],
            password=arg["user_pwd"],
            host=arg["server_name"],
            database=arg["db_name"],
            query={"driver": arg["odbc_name"]},
        )

        engine = sql.create_engine(conn_url)
        query_raw = (
            f"SELECT * FROM [dbo].[{arg['universe']}]\n"
            f"WHERE ddt >= '{self.start_date}' AND ddt <= '{self.end_date}'\n"
            "ORDER BY ddt"
        )

        logger.debug("SQL query: %s", query_raw.replace('\n', ' '))
        df = pd.read_sql_query(query_raw, con=engine)
        engine.dispose()

        if df.empty:
            logger.warning("No rows returned for given date range.")
        else:
            logger.info("Fetched %d rows", len(df))

        return _assign_fld(df)
