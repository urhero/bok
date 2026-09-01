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

import pandas as pd
import sqlalchemy as sql

from config import PARAM

logger = logging.getLogger(__name__)

# universe 테이블명은 파라미터화 불가 (DDL 식별자) → allowlist로 검증
ALLOWED_UNIVERSES = {"clarifi_mxcn1a_afl", "clarifi_mxwo_afl"}


def _build_engine(arg: dict):
    """PARAM dict 로 SQLAlchemy 엔진 생성 (fetch_snp / fetch_country_map 공용)."""
    conn_url = sql.engine.URL.create(
        "mssql+pyodbc",
        username=arg["user_name"],
        password=arg["user_pwd"],
        host=arg["server_name"],
        database=arg["db_name"],
        query={"driver": arg["odbc_name"]},
    )
    return sql.create_engine(conn_url)


def fetch_country_map(param: dict | None = None) -> pd.DataFrame:
    """gvkeyiid -> country 매핑을 반환한다 (지역 중립 랭킹용).

    country 는 정적 속성 (전 이력 복수 국가 종목 0건 확인, 2026-07-28).
    """
    arg = param if param is not None else PARAM
    universe = arg["universe"]
    if universe not in ALLOWED_UNIVERSES:
        raise ValueError(f"Invalid universe '{universe}'. Allowed: {ALLOWED_UNIVERSES}")
    engine = _build_engine(arg)
    df = pd.read_sql_query(
        sql.text(f"SELECT gvkeyiid, MAX(country) AS country FROM [dbo].[{universe}] GROUP BY gvkeyiid"),
        con=engine,
    )
    engine.dispose()
    return df


class GenerateQueryStructure:
    """Fetch raw factor data from SQL Server between *start* and *end* dates."""

    def __init__(self, start_date: str, end_date: str, param: dict | None = None) -> None:
        self._param = param if param is not None else PARAM
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

        engine = _build_engine(arg)

        universe = arg["universe"]
        if universe not in ALLOWED_UNIVERSES:
            raise ValueError(f"Invalid universe '{universe}'. Allowed: {ALLOWED_UNIVERSES}")
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

        # 서버에 따라 ddt가 datetime.date(object)로 내려온다 (kb_global의 date 컬럼).
        # 파이프라인 전역이 datetime64를 가정하므로 fetch 지점에서 정규화한다.
        if not df.empty:
            df["ddt"] = pd.to_datetime(df["ddt"])

        if df.empty:
            logger.warning("No rows returned for given date range.")
        else:
            logger.info("Fetched %d rows", len(df))

        return df
