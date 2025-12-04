from __future__ import annotations

"""팩터 다운로드 및 처리 유틸리티 (Rich 진행바 + 로깅)

주요 기능
--------
* **Rich** 진행바 (로그 라인 위에 표시)
* RichHandler를 사용한 구조화된 로깅 (스크립트로 실행 시)
* 피클링, 랭킹, 분위수 라벨링 등을 위한 헬퍼 함수

2025‑10‑31 개정
-------------------
* 공개 API에 대한 반환 타입 주석 복원:
  * ``assign_factor`` → 4개의 ``pd.DataFrame`` 튜플 **또는** 팩터 히스토리가 
    부족한 경우 4개의 ``None`` 튜플 반환
  * ``download`` → 3개의 ``List[str]``과 ``data_list``의 튜플 반환
* docstring에 더 명시적인 *Returns* 섹션 추가
* 다른 로직 변경 없음
"""


from pathlib import Path
from typing import Any, List, Tuple
from port.query_structure import GenerateQueryStructure
from rich.progress import track

import numpy as np
import pandas as pd
import logging
import pickle
import time

# ----------------------------------------------------------------------------
# 로깅 설정 (스크립트로 실행될 때만 사용)
# ----------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# =============================================================================
# 범용 헬퍼 함수
# =============================================================================

def _dump_pickle(obj: Any, path: Path, *, protocol: int = pickle.HIGHEST_PROTOCOL) -> None:
    """객체를 피클 형식으로 저장
    
    파일을 쓰기 전에 상위 디렉토리가 존재하는지 확인하므로,
    호출자는 디렉토리 트리를 걱정하지 않고 대상 파일 경로만 전달하면 됨
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(pickle.dumps(obj, protocol=protocol))

# =============================================================================
# 수치 계산 헬퍼 유틸리티
# =============================================================================

def _scale_rank(series: pd.Series) -> pd.Series:
    """1~n 순위를 1~99 스케일로 변환 (n ≤ 10이면 NaN 반환)"""
    n = len(series)
    if n <= 10:
        return pd.Series(np.nan, index=series.index)
    return (series - 1) * (99 / (n - 1)) + 1  # 하드코딩 (percentile 화)


def _quantile_label(score: float | int | np.floating) -> str | float:
    """점수(1~100)를 Q1~Q5 라벨로 변환; 범위를 벗어나면 np.nan 반환"""
    if not (1 <= score <= 100):
        return np.nan
    return f"Q{int((score - 1) // 20 + 1)}"  # 20점 단위 버킷(5분위 나눔) 하드코딩


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
        query: pd.DataFrame,
        meta: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame] | Tuple[None, None, None, None]:
    """특정 팩터에 대한 섹터/분위수/스프레드 수익률 계산

    Parameters
    ----------
    abbv, order
        팩터 약어 및 순위 방향 (1=오름차순, 0/-1=내림차순)
    query
        전체 원시 팩터 데이터프레임 (M_RETURN 행 포함)
    meta
        팩터 메타데이터와 조인된 query (스타일/순서 조회용)

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
    fld = meta[meta["factorAbbreviation"] == abbv].dropna().reset_index(drop=True)

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
    m_ret = (
        query[query["factorAbbreviation"] == "M_RETURN"].reset_index(drop=True)
        .rename(columns={"val": "M_RETURN"})
        .drop(columns=["factorAbbreviation"])
    )

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

    # 순위를 1~99 점수로 변환
    merged["score"] = merged.groupby(["ddt", "sec"])["rank"].transform(_scale_rank)
    # 점수를 Q1~Q5 분위수 라벨로 변환
    merged["quantile"] = merged["score"].apply(_quantile_label)
    merged = merged.dropna(subset=["quantile"])

    # ------------------------------------------------------------------
    # 5. 섹터 및 시장 분위수 수익률 계산
    # ------------------------------------------------------------------

    # 섹터별 분위수 평균 수익률 계산
    sector_ret = (
        merged.groupby(["ddt", "sec", "quantile"])["M_RETURN"].mean().unstack(fill_value=0)
    ).groupby("sec").mean().T

    # 전체 시장의 분위수별 평균 수익률 계산 (모든 섹터 포함?)
    quantile_ret = merged.groupby(["ddt", "quantile"])["M_RETURN"].mean().unstack(fill_value=0)

    # ------------------------------------------------------------------
    # 6. Q1‑Q5 스프레드 계산 (롱‑숏 전략)
    # ------------------------------------------------------------------
    # Q1(최고) - Q5(최저) 수익률 차이 계산
    spread = pd.DataFrame({abbv: quantile_ret.iloc[:, 0] - quantile_ret.iloc[:, -1]})
    spread = _add_initial_zero(spread)

    return sector_ret, quantile_ret, spread, merged

# =============================================================================
# 다운로드 드라이버 – 쿼리, 처리, 저장 오케스트레이션
# =============================================================================

def download(
    start_date: str,
    end_date: str,
    *,
    info_path: Path | str = "data/factor_info.csv",
    out_dir: Path | str | None = None,
) -> Tuple[List[str], List[str], List[str], List[Any]]:
    """전체 파이프라인 실행 및 4개의 피클 파일을 디스크에 저장

    Returns
    -------
    tuple
        (``abbr_list``, ``name_list``, ``style_list``, ``data_list``)
    """

    # 출력 디렉토리 설정 (지정되지 않으면 기본 경로 사용)
    out_dir = Path(out_dir) if out_dir else Path(__file__).resolve().parent.parent.parent / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Pickles → %s", out_dir)

    # 1️⃣ 날짜 범위 내 원시 팩터 데이터 가져오기
    t0 = time.time()
    query = (
        GenerateQueryStructure(start_date, end_date)
        .fetch_snp()  # S&P 데이터 가져오기
        .drop_duplicates(
            subset=["ddt", "gvkeyiid", "factorAbbreviation", "val"],
            keep="last",  # 중복 시 마지막 값 유지(그럴까? 쿼리 단계에서 하는 것 보다 빠른가?)
            ignore_index=True,
        )
    )
    logger.info(f"Query fetched in {time.time() - t0:.2f}s")

    # 2️⃣ 메타데이터(순서/스타일/이름)와 조인
    t1 = time.time()
    info = pd.read_csv(info_path)
    meta = query.merge(info, on="factorAbbreviation", how="inner")

    abbrs, orders = info.factorAbbreviation.tolist(), info.factorOrder.tolist()

    # 3️⃣ Rich 진행바와 함께 팩터 할당
    data_list: List[Any] = []
    for abbr, order in track(zip(abbrs, orders), total=len(abbrs), description="Assigning factors"):
        data_list.append(_assign_factor(abbr, order, query, meta))
    logger.info(f"Factors assigned in {time.time() - t1:.2f}s")

    # 4️⃣ 결과를 피클 파일로 저장
    _dump_pickle(abbrs, out_dir / "list_abbv.pkl")  # 팩터 약어 리스트
    _dump_pickle(info.factorName.tolist(), out_dir / "list_name.pkl")  # 팩터 이름 리스트
    _dump_pickle(info.styleName.tolist(), out_dir / "list_style.pkl")  # 스타일 이름 리스트
    _dump_pickle(data_list, out_dir / "list_data.pkl")  # 계산된 데이터 리스트

    logger.info("Done – %d factors processed", len(abbrs))
    return abbrs, info.factorName.tolist(), info.styleName.tolist(), data_list