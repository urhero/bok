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

from config import PARAM

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------------

def _extract_parentheses(text: str) -> str:
    """Return substring inside the first pair of parentheses, else *text*."""
    match = re.search(r"\((.*?)\)", text)
    return match.group(1) if match else text


# ----------------------------------------------------------------------------
# Core class
# ----------------------------------------------------------------------------

class GenerateQueryStructure:
    """Fetch raw factor data from SQL Server between *start* and *end* dates."""

    _param: Dict[str, Any] = PARAM  # centralised DB credentials/settings

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
            f"WITH RankedData AS (\n"
            f"    SELECT \n"
            f"        gvkeyiid, \n"
            f"        ticker, \n"
            f"        isin, \n"
            f"        ddt, \n"
            f"        val,\n"
            f"        fld,\n"
            # 1. fld_name 추출 로직: 괄호가 있으면 앞부분 추출, 없으면 fld 전체 사용
            f"        CASE \n"
            f"            WHEN CHARINDEX('(', fld) > 0 \n"
            f"            THEN RTRIM(LTRIM(LEFT(fld, CHARINDEX('(', fld) - 1)))\n"
            f"            ELSE fld\n"
            f"        END AS fld_name,\n"
            f"        \n"
            # 2. factorAbbreviation 추출 로직: 괄호가 있으면 안의 약어 추출, 없으면(e.g. MXWO_WGT, M_RETURN) fld 전체 사용
            f"        CASE \n"
            f"            WHEN CHARINDEX('(', fld) > 0 AND CHARINDEX(')', fld) > CHARINDEX('(', fld)\n"
            f"            THEN SUBSTRING(\n"
            f"                    fld, \n"
            f"                    CHARINDEX('(', fld) + 1, \n"
            f"                    CHARINDEX(')', fld) - CHARINDEX('(', fld) - 1\n"
            f"                 )\n"
            f"            ELSE fld\n"
            f"        END AS factorAbbreviation,\n"
            f"        \n"
            f"        sec, \n"
            f"        country, \n"
            f"        updated_at, \n"
            f"        \n"
            # 3. 중복 검사 기준: 원래의 fld 값을 사용
            f"        ROW_NUMBER() OVER (\n"
            f"            PARTITION BY gvkeyiid, ddt, fld \n"
            f"            ORDER BY updated_at DESC \n"
            f"        ) as rn\n"
            f"    FROM [dbo].[{arg['universe']}]\n"
            f"    WHERE ddt >= '{self.start_date}' AND ddt <= '{self.end_date}'\n"
            f")\n"
            f"SELECT \n"
            f"    gvkeyiid, \n"
            f"    ticker, \n"
            f"    isin, \n"
            f"    ddt, \n"
            f"    val,\n"
            f"    fld_name, \n"
            f"    factorAbbreviation, \n"
            f"    sec, \n"
            f"    country, \n"
            f"    updated_at \n"
            f"FROM RankedData\n"
            f"WHERE rn = 1\n"
            f"ORDER BY factorAbbreviation, ddt"
        )

        logger.debug("SQL query: %s", query_raw.replace('\n', ' '))
        df = pd.read_sql_query(query_raw, con=engine)
        engine.dispose()

        if df.empty:
            logger.warning("No rows returned for given date range.")
        else:
            logger.info("Fetched %d rows", len(df))

        return df
