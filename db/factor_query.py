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
from typing import Any, Dict

import pandas as pd
import sqlalchemy as sql

from config import PARAM

logger = logging.getLogger(__name__)

class GenerateQueryStructure:
    """Fetch raw factor data from SQL Server between *start* and *end* dates."""

    def __init__(self, start_date: str, end_date: str) -> None:
        self._param = PARAM
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

        # universe 테이블명은 파라미터화 불가 (DDL 식별자) → 허용된 값만 사용
        universe = arg["universe"]
        query_raw = sql.text(
            f"WITH RankedData AS ("
            f"    SELECT "
            f"        gvkeyiid, ticker, isin, ddt, val, fld,"
            f"        CASE "
            f"            WHEN CHARINDEX('(', fld) > 0 AND CHARINDEX(')', fld) > CHARINDEX('(', fld)"
            f"            THEN SUBSTRING(fld, CHARINDEX('(', fld) + 1, CHARINDEX(')', fld) - CHARINDEX('(', fld) - 1)"
            f"            ELSE fld"
            f"        END AS factorAbbreviation,"
            f"        sec, country,"
            f"        ROW_NUMBER() OVER (PARTITION BY gvkeyiid, ddt, fld ORDER BY updated_at DESC) as rn"
            f"    FROM [dbo].[{universe}]"
            f"    WHERE ddt >= :start_date AND ddt <= :end_date"
            f") "
            f"SELECT gvkeyiid, ticker, isin, ddt, val, factorAbbreviation, sec, country "
            f"FROM RankedData WHERE rn = 1 "
            f"ORDER BY factorAbbreviation, ddt"
        )

        logger.debug("SQL query with params: start=%s, end=%s", self.start_date, self.end_date)
        df = pd.read_sql_query(query_raw, con=engine, params={"start_date": self.start_date, "end_date": self.end_date})
        engine.dispose()

        if df.empty:
            logger.warning("No rows returned for given date range.")
        else:
            logger.info("Fetched %d rows", len(df))

        return df
