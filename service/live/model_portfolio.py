# -*- coding: utf-8 -*-

# ═══════════════════════════════════════════════════════════════════════════════
# model_portfolio.py - 모델 포트폴리오(MP) 생성 파이프라인
# ═══════════════════════════════════════════════════════════════════════════════

"""
엔드투엔드 팩터 파이프라인 (v4-complete)
=======================================

【이 파일의 역할】
- 200+ 팩터 데이터를 분석하여 최종 투자 포트폴리오(MP) 생성
- README.md의 [2️⃣~4️⃣] 전체 과정을 구현

【비유】
- 200명의 학생(팩터) 중에서 성적이 좋은 5명을 선발하고
- 그 5명의 성적표를 조합하여 최종 반 편성(포트폴리오) 결정

【주요 단계】
1. [2️⃣] 팩터 후보군 선정: 200+ 팩터를 5분위로 나누고 성과 계산
2. [3️⃣] 최종 팩터 선정: 스타일별 최고 팩터 선택 + 2-팩터 믹스
3. [4️⃣] MP 구성: 스타일 제약(25%) 하에 최적 비중 계산

【처리 흐름】
Parquet 파일 → 팩터 분석 → 성과 랭킹 → 최적화 → CSV 출력

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

# ───────────────────────────────────────────────────────────────────────────────
# 【라이브러리 임포트】
# ───────────────────────────────────────────────────────────────────────────────
import logging  # 로그 기록
import math     # 수학 함수 (제곱, 로그 등)
import time     # 실행 시간 측정
from pathlib import Path  # 파일 경로 처리
from typing import Any, Dict, List, Tuple, Union  # 타입 힌트

import numpy as np   # 수치 계산 (배열, 행렬 연산) - 빠른 계산을 위한 핵심 도구
import pandas as pd  # 데이터 테이블 처리 (엑셀 같은 표 형식)
from rich.progress import track  # 진행바 표시

from config import PARAM  # 설정 파일

# ───────────────────────────────────────────────────────────────────────────────
# 로깅 설정
# ───────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# 【수치 계산 헬퍼 유틸리티】
# ═══════════════════════════════════════════════════════════════════════════════

def prepend_start_zero(series: pd.DataFrame) -> pd.DataFrame:
    """시계열 데이터 맨 앞에 0 추가 (누적 수익률 계산의 기준선)

    【목적】
    - 누적 수익률 계산 시 시작점을 0%로 맞추기

    【비유】
    - 달리기 시합의 출발선(0m) 표시
    - 2018-01-31부터 데이터가 있으면 2017-12-31에 0 추가

    【예시】
    입력:
        2018-01-31: 0.05
        2018-02-28: 0.03

    출력:
        2017-12-31: 0.00  ← 추가됨!
        2018-01-31: 0.05
        2018-02-28: 0.03

    【파라미터】
    series : pd.DataFrame
        날짜가 인덱스인 시계열 데이터

    【반환값】
    pd.DataFrame
        맨 앞에 0이 추가되고 정렬된 시계열
    """
    # DateOffset(months=1): 한 달 전 날짜 계산
    # 예: 2018-01-31 → 2017-12-31
    series.loc[series.index[0] - pd.DateOffset(months=1)] = 0

    # sort_index(): 날짜 순서로 정렬 (맨 앞에 추가된 0이 첫 행이 됨)
    return series.sort_index()


# ═══════════════════════════════════════════════════════════════════════════════
# 【핵심 팩터 분석 로직】
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_factor_stats(
    factor_abbr: str,
    sort_order: int,
    factor_data_df: pd.DataFrame,
    test_mode: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame] | Tuple[None, None, None, None]:
    """특정 팩터에 대한 5분위 포트폴리오 구성 및 성과 계산

    【목적】
    - 종목들을 팩터값 기준으로 5개 그룹(Q1~Q5)으로 나누기
    - 각 그룹의 평균 수익률 계산
    - Q1(최고) - Q5(최저) 스프레드 계산

    【비유】
    - 100명 학생을 수학 성적순으로 5개 반(각 20명)으로 나누기
    - Q1(상위 20%) vs Q5(하위 20%) 평균 성적 차이 = 스프레드

    【주요 단계】
    1. 팩터값에 1개월 래그 적용 (전월 값으로 당월 투자)
    2. 섹터별로 팩터값 순위 매기기
    3. 순위를 백분위(0~100)로 변환
    4. 백분위를 5분위(Q1~Q5)로 버킷화
    5. 각 분위의 평균 수익률 계산
    6. Q1-Q5 스프레드 시계열 생성

    【README.md 연결】
    - [2️⃣-2] 팩터별 5분위 포트폴리오 구성

    Parameters
    ----------
    factor_abbr : str
        팩터 약어 (예: "ROIC", "ROE")
    sort_order : int
        순위 방향
        - 1: 오름차순 (값이 클수록 좋음, 예: 수익률)
        - 0/-1: 내림차순 (값이 작을수록 좋음, 예: PER)
    factor_data_df : pd.DataFrame
        해당 팩터의 데이터 (이미 필터링됨, M_RETURN 포함)
        필수 컬럼: gvkeyiid, ticker, ddt, sec, val, M_RETURN
    test_mode : bool
        테스트 모드 여부 (True면 최소 개수 체크 생략)

    Returns
    -------
    Tuple[DataFrame, DataFrame, DataFrame, DataFrame] | tuple[None, None, None, None]
        성공 시:
        - sector_ret: 섹터 × 분위수(Q1‑Q5)별 평균 월간 수익률
        - quantile_ret: 분위수(Q1‑Q5)별 전체 시장 수익률
        - spread: 롱‑숏(Q1‑Q5) 월간 성과 시계열
        - merged: 계산에 사용된 종목 수준 데이터 (quantile 컬럼 포함)

        실패 시 (데이터 부족):
        - (None, None, None, None)
    """
    logger.debug(f"[Trace] Processing factor {factor_abbr}. Data shape: {factor_data_df.shape}")

    # ═══════════════════════════════════════════════════════════════════════════
    # 【1단계: 팩터 시계열 수집 및 래그 적용】
    # ═══════════════════════════════════════════════════════════════════════════
    # 데이터 정제: NaN 제거, 인덱스 리셋
    factor_data_df = factor_data_df.dropna().reset_index(drop=True)

    # ───────────────────────────────────────────────────────────────────────────
    # 히스토리 충분성 검사
    # ───────────────────────────────────────────────────────────────────────────
    # 날짜가 2개 이하면 분석 불가 (최소 3개월 필요)
    # 예: 2024-01, 2024-02 만 있으면 스킵
    if len(factor_data_df['ddt'].unique()) <= 2:
        logger.warning("Skipping %s – insufficient history", factor_abbr)
        return None, None, None, None

    # ───────────────────────────────────────────────────────────────────────────
    # 【래그(Lag) 적용】 ⚠️ 초보자 주의!
    # ───────────────────────────────────────────────────────────────────────────
    # 각 종목별로 1개월 래그 적용 (전월 팩터 값을 당월에 사용)
    #
    # 【래그란?】
    # - 시간을 한 칸 뒤로 미루는 것
    # - 예: 12월 성적을 1월에 사용
    #
    # 【왜 래그를 적용하나?】
    # - 현실에서는 이번달 팩터값을 이번달 말에야 알 수 있음
    # - 전월 팩터값으로 당월 투자 (미래 정보 사용 방지)
    #
    # 【예시】
    # 원래:
    #   종목A, 2024-01: val=10, M_RETURN=5%
    #   종목A, 2024-02: val=12, M_RETURN=3%
    #
    # shift(1) 적용 후:
    #   종목A, 2024-01: ROIC=NaN  (첫 달은 이전 값이 없음)
    #   종목A, 2024-02: ROIC=10   (2024-01의 val을 2024-02에 사용)
    #
    # groupby("gvkeyiid"): 종목별로 따로 shift (다른 종목과 섞이지 않음)
    factor_data_df[factor_abbr] = factor_data_df.groupby("gvkeyiid")["val"].shift(1)

    # ───────────────────────────────────────────────────────────────────────────
    # 데이터 정리
    # ───────────────────────────────────────────────────────────────────────────
    # NaN 제거 + 불필요한 컬럼 제거
    merged_df = (
        factor_data_df
        .dropna(subset=[factor_abbr, "M_RETURN"])  # 팩터값과 수익률이 둘 다 있는 행만 유지
        .drop(columns=["val", "factorAbbreviation"])  # 원본 val, factorAbbreviation 제거
        .reset_index(drop=True)  # 인덱스 재설정
    )

    # ═══════════════════════════════════════════════════════════════════════════
    # 【2단계: 섹터 내 순위 매기기 및 5분위 버킷 할당】 ⭐ 핵심!
    # ═══════════════════════════════════════════════════════════════════════════

    # ───────────────────────────────────────────────────────────────────────────
    # 【Rank (순위) 계산】
    # ───────────────────────────────────────────────────────────────────────────
    # 같은 날짜, 같은 섹터 내에서 팩터 값의 순위 계산
    #
    # 【비유】
    # - 각 반(섹터)에서 시험 성적 순위 매기기
    # - 2024-01월, IT 섹터: 삼성전자 1위, SK하이닉스 2위, ...
    #
    # groupby(["ddt", "sec"]): 날짜+섹터별로 묶어서
    # .rank(): 순위 매기기
    # method="average": 동점자는 평균 순위 (예: 1등 2명이면 1.5등으로)
    # ascending=bool(sort_order): 정렬 방향 결정
    merged_df["rank"] = (
        merged_df.groupby(["ddt", "sec"])[factor_abbr].rank(method="average", ascending=bool(sort_order))
    )

    # ───────────────────────────────────────────────────────────────────────────
    # 날짜+섹터별 종목 개수 계산
    # ───────────────────────────────────────────────────────────────────────────
    # transform("count"): 각 그룹의 개수를 모든 행에 브로드캐스트
    # 예: IT 섹터에 10개 종목 → 10개 종목 모두에 count=10 할당
    count_series = merged_df.groupby(["ddt", "sec"])[factor_abbr].transform("count")

    # ───────────────────────────────────────────────────────────────────────────
    # 【Percentile (백분위) 변환】 ⚠️ 초보자 주의!
    # ───────────────────────────────────────────────────────────────────────────
    # Rank (1~N)을 Percentile (0~100)로 변환
    #
    # 【공식】
    # Percentile = (Rank - 1) / (Count - 1) × 100
    #
    # 【왜 -1을 하나?】
    # - 0%부터 시작하게 만들기 위해
    # - 1등 (Rank=1) → (1-1)/(N-1)×100 = 0%
    # - 꼴찌 (Rank=N) → (N-1)/(N-1)×100 = 100%
    #
    # 【예시】 10명인 경우
    # - 1등: (1-1)/(10-1)×100 = 0%
    # - 2등: (2-1)/(10-1)×100 = 11.1%
    # - ...
    # - 10등: (10-1)/(10-1)×100 = 100%
    merged_df["percentile"] = (merged_df["rank"] - 1) / (count_series - 1) * 100

    # 데이터가 너무 적으면 (10개 이하) NaN 처리 (신뢰도 낮음)
    # 테스트 모드에서는 이 체크 생략
    if not test_mode:
        merged_df.loc[count_series <= 10, "percentile"] = np.nan

    # ───────────────────────────────────────────────────────────────────────────
    # 【Quantile (분위) 버킷화】 ⭐ 가장 중요!
    # ───────────────────────────────────────────────────────────────────────────
    # Percentile (0~100)을 5개 구간(Q1~Q5)으로 나누기
    #
    # 【비유】
    # - 100명을 성적순으로 5개 반으로 나누기
    # - Q1: 상위 20% (0~20점)
    # - Q2: 20~40%
    # - Q3: 40~60%
    # - Q4: 60~80%
    # - Q5: 하위 20% (80~100%)
    #
    # pd.cut(): 연속된 값을 구간으로 나누는 함수
    # bins=[0, 20, 40, 60, 80, 105]: 구간 경계 (105는 100 포함을 위해)
    # labels=["Q1", ..., "Q5"]: 각 구간의 이름
    # include_lowest=True: 0도 포함
    # right=True: 오른쪽 경계 포함 (예: 20은 Q1에 포함)
    labels = ["Q1", "Q2", "Q3", "Q4", "Q5"]
    merged_df["quantile"] = pd.cut(
        merged_df["percentile"],
        bins=[0, 20, 40, 60, 80, 105],  # 100 포함을 위해 105
        labels=labels,
        include_lowest=True,
        right=True
    )

    # quantile이 NaN인 행 제거 (percentile이 없으면 quantile도 없음)
    merged_df = merged_df.dropna(subset=["quantile"])

    # 메모리 절약: 중간 계산 컬럼 제거
    merged_df = merged_df.drop(columns=["rank", "percentile"])

    # ═══════════════════════════════════════════════════════════════════════════
    # 【3단계: 분위별 평균 수익률 계산】
    # ═══════════════════════════════════════════════════════════════════════════

    # ───────────────────────────────────────────────────────────────────────────
    # 섹터별 × 분위별 평균 수익률
    # ───────────────────────────────────────────────────────────────────────────
    # 각 섹터에서 Q1~Q5 그룹의 평균 수익률 계산
    # 예: IT 섹터 Q1 = 5%, IT 섹터 Q5 = -2%
    sector_return_df = (
        merged_df.groupby(["ddt", "sec", "quantile"], observed=False)["M_RETURN"]
        .mean()  # 날짜+섹터+분위별 평균
        .unstack(fill_value=0)  # 분위를 컬럼으로 변환 (Q1, Q2, Q3, Q4, Q5)
    ).groupby("sec").mean().T  # 섹터별 전체 기간 평균, 전치(Transpose)

    # ───────────────────────────────────────────────────────────────────────────
    # 전체 시장의 분위별 평균 수익률
    # ───────────────────────────────────────────────────────────────────────────
    # 모든 섹터를 합쳐서 Q1~Q5 그룹의 평균 수익률 계산
    quantile_return_df = merged_df.groupby(["ddt", "quantile"], observed=False)["M_RETURN"].mean().unstack(fill_value=0)

    # ═══════════════════════════════════════════════════════════════════════════
    # 【4단계: Q1-Q5 스프레드 계산 (롱-숏 전략)】
    # ═══════════════════════════════════════════════════════════════════════════
    # Q1(최고) - Q5(최저) 수익률 차이 = 팩터의 효과
    #
    # 【비유】
    # - 1등 반 평균 - 꼴찌 반 평균 = 성적 차이
    # - 차이가 크면 이 팩터가 유용함!
    #
    # iloc[:, 0]: 첫 번째 컬럼 (Q1)
    # iloc[:, -1]: 마지막 컬럼 (Q5)
    spread_series = pd.DataFrame({factor_abbr: quantile_return_df.iloc[:, 0] - quantile_return_df.iloc[:, -1]})

    # 맨 앞에 0 추가 (누적 수익률 계산 시 기준선)
    spread_series = prepend_start_zero(spread_series)

    logger.debug(f"[Trace] Factor {factor_abbr} assigned. Sector Ret Shape: {sector_return_df.shape}, Quantile Ret Shape: {quantile_return_df.shape}")
    return sector_return_df, quantile_return_df, spread_series, merged_df


# ───────────────────────────────────────────────────────────────────────────────
# 전역 경로 설정
# ───────────────────────────────────────────────────────────────────────────────
DATA_DIR = Path.cwd() / "data"  # 데이터 폴더
DATA_DIR.mkdir(parents=True, exist_ok=True)  # 없으면 생성

OUTPUT_DIR = Path.cwd() / "output"  # 출력 폴더
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # 없으면 생성


# ═══════════════════════════════════════════════════════════════════════════════
# 【섹터 필터링 + 롱/숏 라벨 재계산】
# ═══════════════════════════════════════════════════════════════════════════════

def filter_and_label_factors(
    factor_abbr_list: List[str],
    factor_name_list: List[str],
    style_name_list: List[str],
    factor_data_list: List[Tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None]],
) -> Tuple[List[str], List[str], List[str], List[int], List[List[str]], List[pd.DataFrame]]:
    """음의 스프레드를 가진 섹터 제거 및 롱/숏 라벨 재계산

    【목적】
    - 팩터가 역효과를 내는 섹터 제거
    - 실제로 매수(L)/중립(N)/매도(S)할 분위 결정

    【비유】
    - 수학 성적이 국어 점수를 예측하는지 확인
    - 문과반에서는 상관있지만 이과반에서는 역상관 → 이과반 제외
    - 남은 반들에서 상위 20%는 매수, 하위 20%는 매도

    【주요 단계】
    1. 섹터별 Q1-Q5 스프레드 계산
    2. 스프레드가 음수(-)인 섹터 제거
    3. 남은 데이터로 분위별 평균 재계산
    4. 임계값(threshold) 기반으로 L/N/S 라벨 부여

    【README.md 연결】
    - [2️⃣-3] 비투자 섹터 결정
    - [2️⃣-4] 투자 대상 분위(롱/숏) 결정

    Parameters
    ----------
    factor_abbr_list : List[str]
        팩터 약어 리스트
    factor_name_list : List[str]
        팩터 이름 리스트
    style_name_list : List[str]
        스타일 이름 리스트
    factor_data_list : List[Tuple]
        calculate_factor_stats() 결과 리스트
        각 원소: (sector_ret, quantile_ret, spread, raw_df)

    Returns
    -------
    Tuple[List[str], List[str], List[str], List[int], List[List[str]], List[pd.DataFrame]]
        - kept_factor_abbrs: 유지된 팩터 약어
        - kept_name: 유지된 팩터 이름
        - kept_style: 유지된 스타일 이름
        - kept_idx: 유지된 팩터 인덱스
        - dropped_sec: 팩터별 제거된 섹터 리스트
        - filtered_raw_data_list: 필터링된 종목 데이터 (label 컬럼 포함)
    """

    # 결과 저장용 리스트 초기화
    kept_factor_abbrs, kept_name, kept_style, kept_idx = [], [], [], []
    dropped_sec: List[List[str]] = []
    filtered_raw_data_list: List[pd.DataFrame] = []

    # ═══════════════════════════════════════════════════════════════════════════
    # 【각 팩터별로 섹터 필터링 수행】
    # ═══════════════════════════════════════════════════════════════════════════
    for idx, (sector_return_df, _, _, raw_df) in track(
        enumerate(factor_data_list), description="Filtering sectors", total=len(factor_data_list)
    ):
        # ───────────────────────────────────────────────────────────────────────
        # 데이터 유효성 검사
        # ───────────────────────────────────────────────────────────────────────
        if sector_return_df is None or raw_df is None:
            logger.debug("Factor %d skipped – no data", idx)
            continue

        # ───────────────────────────────────────────────────────────────────────
        # 【1단계: 음의 스프레드 섹터 식별 및 제거】
        # ───────────────────────────────────────────────────────────────────────
        # sector_return_df 전치: 섹터를 행으로, 분위를 컬럼으로
        tmp = sector_return_df.T.reset_index()

        # Q1-Q5 스프레드 계산 (양수여야 정상)
        # 예: IT 섹터 Q1=5%, Q5=2% → spread=3% (양수, OK)
        #     금융 섹터 Q1=1%, Q5=3% → spread=-2% (음수, 제거 대상!)
        tmp["spread"] = tmp["Q1"] - tmp["Q5"]

        # 스프레드가 음수인 섹터 찾기
        # 음수 = 이 팩터가 해당 섹터에서 역효과를 냄
        to_drop = tmp.loc[tmp["spread"] < 0, "sec"].tolist()

        # 해당 섹터의 모든 종목 제거
        raw_clean = raw_df[~raw_df["sec"].isin(to_drop)].reset_index(drop=True)

        # 모든 섹터가 제거되면 이 팩터는 폐기
        if raw_clean.empty:
            logger.debug("Factor %d discarded – all sectors dropped", idx)
            continue

        # ───────────────────────────────────────────────────────────────────────
        # 【2단계: 섹터 제거 후 분위별 평균 재계산】
        # ───────────────────────────────────────────────────────────────────────
        # 남은 데이터로 분위별 수익률 다시 계산
        q_ret = raw_clean.groupby(["ddt", "quantile"], observed=False)["M_RETURN"].mean().unstack(fill_value=0)

        # 전체 기간 평균 수익률 계산 (시계열 → 하나의 숫자)
        q_mean = q_ret.mean(axis=0).to_frame("mean")

        # ───────────────────────────────────────────────────────────────────────
        # 【3단계: 롱/숏 라벨 결정】 ⚠️ 복잡!
        # ───────────────────────────────────────────────────────────────────────
        # 임계값(threshold) 계산: 스프레드의 10%
        # 예: Q1=5%, Q5=0% → spread=5% → thresh=0.5%
        thresh = abs(q_mean.loc["Q1", "mean"] - q_mean.loc["Q5", "mean"]) * 0.10

        # ─────────────────────────────────────────────────────────────────────
        # 롱(Long) 라벨 할당
        # ─────────────────────────────────────────────────────────────────────
        # Q1부터 시작해서 수익률이 (Q1 - threshold)보다 높은 분위에 롱(+1) 부여
        # cumprod(): 누적곱 (한번 0이 나오면 이후 모두 0)
        # 예: Q1=5%, Q2=4.6%, Q3=3% (thresh=0.5%)
        #     Q1: 5% > 4.5% → True → 1 → cumprod=1 → label=+1 (롱)
        #     Q2: 4.6% > 4.5% → True → 1 → cumprod=1 → label=+1 (롱)
        #     Q3: 3% > 4.5% → False → 0 → cumprod=0 → label=0 (중립)
        q_mean["long"] = (q_mean["mean"] > q_mean.loc["Q1", "mean"] - thresh).astype(int).cumprod()

        # ─────────────────────────────────────────────────────────────────────
        # 숏(Short) 라벨 할당
        # ─────────────────────────────────────────────────────────────────────
        # Q5부터 거꾸로 올라가면서 수익률이 (Q5 + threshold)보다 낮은 분위에 숏(-1) 부여
        # [::-1]: 역순 정렬, cumprod(), 다시 역순, -1 곱하기
        q_mean["short"] = (q_mean["mean"] < q_mean.loc["Q5", "mean"] + thresh).astype(int) * -1
        q_mean["short"] = q_mean["short"].abs()[::-1].cumprod()[::-1] * -1

        # ─────────────────────────────────────────────────────────────────────
        # 최종 라벨 = 롱 + 숏
        # ─────────────────────────────────────────────────────────────────────
        # 예: Q1=+1, Q2=+1, Q3=0, Q4=0, Q5=-1
        q_mean["label"] = q_mean["long"] + q_mean["short"]

        # ───────────────────────────────────────────────────────────────────────
        # 【4단계: 라벨을 종목 데이터에 매핑】
        # ───────────────────────────────────────────────────────────────────────
        # quantile → label 매핑 딕셔너리 생성
        # 예: {"Q1": 1, "Q2": 1, "Q3": 0, "Q4": 0, "Q5": -1}
        label_map = q_mean["label"].to_dict()

        # 각 종목의 quantile에 맞는 label 할당
        # 예: 삼성전자가 Q1이면 label=+1 (롱)
        raw_clean["label"] = raw_clean["quantile"].map(label_map)

        # label이 없는 행 제거 (혹시 모를 매칭 실패)
        merged = raw_clean.dropna(subset=["label"])

        # ───────────────────────────────────────────────────────────────────────
        # 결과 저장
        # ───────────────────────────────────────────────────────────────────────
        kept_factor_abbrs.append(factor_abbr_list[idx])
        kept_name.append(factor_name_list[idx])
        kept_style.append(style_name_list[idx])
        kept_idx.append(idx)
        dropped_sec.append(to_drop)
        filtered_raw_data_list.append(merged)

    logger.info("Sector filter retained %d / %d factors", len(kept_idx), len(factor_abbr_list))
    return kept_factor_abbrs, kept_name, kept_style, kept_idx, dropped_sec, filtered_raw_data_list


# ═══════════════════════════════════════════════════════════════════════════════
# 【하락 상관관계 (Downside Correlation)】
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_downside_correlation(df: pd.DataFrame, min_obs: int = 20) -> pd.DataFrame:
    """하락 국면에서의 상관관계 계산 (Downside Correlation)

    【목적】
    - 시장이 나쁠 때 함께 떨어지는 정도 측정
    - 일반 상관관계보다 위험 관리에 더 중요

    【비유】
    - 평소에는 친구들이 각자 공부 (독립적)
    - 중간고사(위기)에서는 모두 같이 떨어짐 → 하락 상관관계 높음
    - 포트폴리오: 하락장에서 함께 떨어지면 분산 효과 없음

    【일반 상관관계 vs 하락 상관관계】
    - 일반 상관관계: 모든 시점의 상관계수
    - 하락 상관관계: 수익률이 음수인 시점만의 상관계수

    【왜 하락 국면만?】
    - 투자자는 손실에 더 민감 (Downside Risk)
    - 하락장에서 상관관계 높으면 위험
    - 분산 투자가 가장 필요한 시점에 효과가 없음

    【NumPy 벡터화】
    - pandas 반복문 대신 NumPy 행렬 연산 사용
    - 속도: 50배+ 빠름
    - 메모리: 효율적

    Parameters
    ----------
    df : pd.DataFrame
        월간 수익률 행렬 (행=날짜, 열=팩터)
    min_obs : int
        최소 관측 횟수 (기본 20)
        이보다 적으면 NaN 반환 (신뢰도 낮음)

    Returns
    -------
    pd.DataFrame
        하락 상관관계 행렬 (팩터 × 팩터)
        대각선은 1.0 (자기 자신)
    """

    # ═══════════════════════════════════════════════════════════════════════════
    # 【1단계: 데이터 준비】
    # ═══════════════════════════════════════════════════════════════════════════
    # pandas → NumPy 변환 (빠른 계산을 위해)
    data = df.to_numpy(dtype=np.float64)  # 64비트 실수형
    n_cols = data.shape[1]  # 팩터 개수
    cols = df.columns  # 컬럼 이름 저장

    # 결과 행렬 초기화 (NaN으로 채움)
    out = np.full((n_cols, n_cols), np.nan, dtype=np.float64)

    # ═══════════════════════════════════════════════════════════════════════════
    # 【2단계: 각 팩터별로 하락 상관관계 계산】
    # ═══════════════════════════════════════════════════════════════════════════
    for i in range(n_cols):
        # ───────────────────────────────────────────────────────────────────────
        # i번째 팩터가 음수인 시점 찾기
        # ───────────────────────────────────────────────────────────────────────
        # mask: True/False 배열
        # 예: [False, True, False, True] → 2번째, 4번째 달에 음수 수익률
        mask = data[:, i] < 0

        # 음수인 시점이 최소 개수(min_obs) 이상이어야 신뢰 가능
        # 예: 20번 이상 하락해야 상관계수 계산
        if mask.sum() >= min_obs:
            # ───────────────────────────────────────────────────────────────────
            # 하락 시점만 추출
            # ───────────────────────────────────────────────────────────────────
            # subset: i번째 팩터가 음수인 시점의 모든 팩터 수익률
            # 예: 2월, 5월, 9월에 팩터A가 음수 → 그 3개월의 모든 팩터 데이터
            subset = data[mask, :]

            # ───────────────────────────────────────────────────────────────────
            # 상관계수 계산
            # ───────────────────────────────────────────────────────────────────
            # 【상관계수 공식】
            # corr(X, Y) = cov(X, Y) / (std(X) × std(Y))
            #            = E[(X - mean_X)(Y - mean_Y)] / (std_X × std_Y)

            # 각 팩터의 평균 계산 (하락 시점만)
            means = np.nanmean(subset, axis=0)

            # 각 팩터의 표준편차 계산 (하락 시점만)
            # ddof=1: 표본 표준편차 (N-1로 나눔)
            stds = np.nanstd(subset, axis=0, ddof=1)

            # 중심화 (평균 빼기)
            # 예: [1, 2, 3] - 2 = [-1, 0, 1]
            centered = subset - means

            # 공분산 계산
            # centered[:, i:i+1]: i번째 팩터의 중심화 값 (열 벡터)
            # centered * centered[:, i:i+1]: 각 팩터와 i번째 팩터의 곱
            cov_with_i = np.nanmean(centered * centered[:, i:i+1], axis=0)

            # 상관계수 = 공분산 / (표준편차 곱)
            corr_row = cov_with_i / (stds * stds[i])

            # i번째 행에 결과 저장
            out[i, :] = corr_row

    # ═══════════════════════════════════════════════════════════════════════════
    # 【3단계: NumPy → pandas 변환 후 반환】
    # ═══════════════════════════════════════════════════════════════════════════
    return pd.DataFrame(out, index=cols, columns=cols)

def construct_long_short_df(
    labeled_data_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    raw_df = labeled_data_df[labeled_data_df["ddt"] >= "2017-12-31"].reset_index(drop=True).copy()
    raw_df["signal"] = raw_df["label"].map({1: "L", 0: "N", -1: "S"})
    raw_df["num"] = raw_df.groupby(["ddt", "signal"])["signal"].transform("count")
    # return_weight은 포트폴리오 비중임, 수익률 계산용 #?
    raw_df["return_weight"] = 1 / raw_df["num"] * raw_df["label"]
    # tvr_df 는 턴오버 계산용 wgt임. 실제 턴오버는 trading_friction #?
    raw_df["turnover_weight"] = abs(raw_df["return_weight"])
    long_df = raw_df[raw_df["signal"] == "L"].reset_index(drop=True)
    short_df = raw_df[raw_df["signal"] == "S"].reset_index(drop=True)
    return long_df, short_df


def calculate_vectorized_return(
    portfolio_data_df: pd.DataFrame,
    factor_abbr: str,
    cost_bps: float = 30.0
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    weight_matrix_df = portfolio_data_df.pivot_table(index="ddt", columns="gvkeyiid", values="return_weight")
    rtn_df = portfolio_data_df.pivot_table(index="ddt", columns="gvkeyiid", values="M_RETURN")
    rtn_df.iloc[0] = 0
    # tvr_df 는 턴오버 계산용 wgt임. 실제 턴오버는 trading_friction #?
    turnover_weight_df = portfolio_data_df.pivot_table(index="ddt", columns="gvkeyiid", values="turnover_weight")
    sgn_df = np.sign(weight_matrix_df)

    r = rtn_df.sort_index()
    w = turnover_weight_df.reindex(r.index)
    w0 = turnover_weight_df.copy()
    is_rebal = w.notna().any(axis=1).fillna(False)  # 각 날짜별 NA가 아닌게 하나라도 있으면 True #?
    block_id = is_rebal.cumsum().astype(int)  # 리밸런싱 블럭 1,2,3,.... #?
    cumulative_growth_block = (1 + sgn_df * r).groupby(block_id).cumprod()  # 블럭내에서 누적 곱

    denom = (w0 * cumulative_growth_block).sum(axis=1)  # 각 날짜 비중 합
    w_pre = (w0 * cumulative_growth_block).div(denom, axis=0)  # 각 날짜 비중 100%로 조정

    weight_matrix_df.iloc[0] = w0.loc[weight_matrix_df.index[0]]  # 첫날 비중
    rebal_in_r = r.index.intersection(turnover_weight_df.index)  # 리밸런싱 웨이트와 수익률 날짜(인덱스) 교집합으로 리밸런싱 날짜 선택
    turnover = 1 * (w.shift(-1).loc[rebal_in_r] - w_pre.loc[rebal_in_r]).abs().sum(axis=1)  # 리밸런싱 날짜의 웨이트 차이
    turnover = turnover.reindex(r.index).fillna(0)  # 리밸런싱 날짜 외의 날짜는 0으로 채움
    trading_friction = (cost_bps / 1e4) * turnover  # 거래비용

    _gross = (weight_matrix_df * r).sum(axis=1)  # 날짜별 수익률 (이미 시프트 되어 있음)
    gross_return_df = _gross.to_frame().rename(columns={0: factor_abbr})  # 날짜별 수익률(거래비용 차감전)

    trading_cost_df = trading_friction.to_frame().rename(columns={0: factor_abbr})  # 날짜별 거래비용, 시리즈를 데이터프레임으로 변환
    _net_df = gross_return_df - trading_cost_df  # 날짜별 수익률(거래비용 차감전) - 거래비용

    return gross_return_df, _net_df, trading_cost_df


def aggregate_factor_returns(
    factor_data_list: List[pd.DataFrame],
    factor_abbr_list: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    list_grs, list_net, list_trc = [], [], []
    for list_raw, factor_abbr in zip(factor_data_list, factor_abbr_list):
        long_port_df, short_port_df = construct_long_short_df(list_raw)
        res_grs_l, res_net_l, res_trc_l = calculate_vectorized_return(long_port_df, factor_abbr)
        res_grs_s, res_net_s, res_trc_s = calculate_vectorized_return(short_port_df, factor_abbr)
        list_grs.append(res_grs_l + res_grs_s)
        list_net.append(res_net_l + res_net_s)
        list_trc.append(res_trc_l + res_trc_s)

    gross_return_df = pd.concat(list_grs, axis=1).dropna(axis=1)
    net_return_df = pd.concat(list_net, axis=1).dropna(axis=1)
    trading_cost_df = pd.concat(list_trc, axis=1).dropna(axis=1)

    return gross_return_df, net_return_df, trading_cost_df


def evaluate_factor_universe(
    factor_abbr_list: List[str],
    factor_name_list: List[str],
    style_name_list: List[str],
    factor_data_list: List[pd.DataFrame],
    test_file: str | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    logger.info("Building monthly return matrix")
    ret_df = aggregate_factor_returns(factor_data_list, factor_abbr_list)[1]
    ret_df.loc[ret_df.index[0]] = 0.0
    ret_df = ret_df.sort_index()  # 날짜 오름차순, 혹시나 싶어서 함 #?

    # 중복된 컬럼 제거 (테스트 모드에서 발생 가능)
    if ret_df.columns.duplicated().any():
        logger.warning("Duplicate factor columns detected, removing duplicates")
        ret_df = ret_df.loc[:, ~ret_df.columns.duplicated(keep='first')]

    valid = ret_df.columns[(ret_df == 0).sum() <= 10]  # 컬럼이 팩터, 수익률 0인 애들이 10개 이하인 팩터가 valid
    ret_df = ret_df[valid]

    meta = (
        pd.DataFrame({"factorAbbreviation": factor_abbr_list, "factorName": factor_name_list, "styleName": style_name_list})
        .set_index("factorAbbreviation")
        .loc[valid]
        .reset_index()
    )  # 롱 형식

    months = len(ret_df) - 1
    meta["cagr"] = ((1 + ret_df).cumprod().iloc[-1] ** (12 / months) - 1).values
    meta["rank_style"] = meta.groupby("styleName")["cagr"].rank(ascending=False)  # 스타일내에서의 랭크
    meta["rank_total"] = meta["cagr"].rank(ascending=False)  # 전체에서의 랭크
    # CAGR 내림차순 정렬
    meta = meta.sort_values("cagr", ascending=False).reset_index(drop=True).rename(columns={"index": "factorAbbreviation"})
    # 테스트 파일이 제공된 경우, 파일명(확장자 제외)을 suffix로 사용
    if test_file:
        suffix = f"_{Path(test_file).stem}"
        meta.to_csv(OUTPUT_DIR / f"meta_data_test{suffix}.csv", index=False)
    else:
        meta.to_csv(OUTPUT_DIR / "meta_data.csv", index=False)
    meta = meta[:50]  # 상위 50개만 선택

    order = meta["factorAbbreviation"].tolist()
    ret_df = ret_df[order]  # 50개 팩터만
    negative_corr = calculate_downside_correlation(ret_df).loc[order, order]  # 50개 팩터간의 하락 상관계수

    logger.info("Return matrix built (%d factors)", len(order))
    logger.info(f"[Trace] Generated Factor Return Matrix. Shape: {ret_df.shape}")
    logger.info(f"[Trace] Generated Negative Correlation Matrix. Shape: {negative_corr.shape}")
    return ret_df, negative_corr, meta


# =============================================================================
# 4️⃣ 2-팩터 믹스 최적화
# =============================================================================
def find_optimal_mix(
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
        # Skip if main and sub are the same factor
        if main == sub:
            logger.warning(f"Skipping mix of {main} with itself")
            continue
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
    logger.info(f"[Trace] Generated Mix Grid for {main}. Size: {len(df_mix)}")
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
def construct_style_portfolios(
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

        df_mix, series_list, *_ = find_optimal_mix(
            factor_rets, row.to_frame().T.reset_index(drop=True), neg_corr
        )
        best_idx = df_mix.nsmallest(1, "rank_total").index[0]
        mixes[tag] = series_list[best_idx].rename(tag)

    style_df = pd.concat(mixes.values(), axis=1)
    style_neg_corr = calculate_downside_correlation(style_df)
    logger.info("Built %d style portfolios", style_df.shape[1])
    return style_df, style_neg_corr


# =============================================================================
# 6️⃣ 팩터 노출도 시뮬레이션
# =============================================================================
def simulate_constrained_weights(
    rtn_df: pd.DataFrame,
    style_list: List[str],
    num_sims: int = 1_000_000,
    style_cap: float = 0.25,
    tol: float = 1e-12,
    test_mode: bool = False,
    batch_size: int = 100_000,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    스타일 weight 제약이 있는 최적 포트폴리오를 몬테카를로 탐색

    Parameters
    ----------
    rtn_df : DataFrame
        Monthly spread return matrix (rows = dates, cols = factorAbbreviation).
    style_list : list[str]
        Style name for every column in `rtn_df`, in the same order.
    num_sims : int, default 1_000_000
        Number of random portfolios to draw.
    style_cap : float, default 0.25
        Maximum weight share per style.
    tol : float, default 1e-12
        Numerical tolerance when checking caps.
    test_mode : bool, default False
        If True, relax style_cap constraint for small datasets.
    batch_size : int, default 100_000
        Batch size for memory-efficient processing.

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
    T = rtn_df.shape[0]
    if len(style_list) != K:
        raise ValueError("length of style_list must equal number of columns in rtn_df")

    # 테스트 모드에서 style_cap 완화 (작은 데이터셋 대응)
    if test_mode:
        style_cap = 1.0  # 100% - 제약 없음
        logger.info(f"Test mode: relaxed style_cap to {style_cap}")

    styles = np.asarray(style_list)

    # ------------------------------------------------------------------
    # 2. Build style mask (S × K) - float32 for memory efficiency
    # ------------------------------------------------------------------
    uniq_styles = np.unique(styles)
    S = len(uniq_styles)

    mask = np.zeros((S, K), dtype=np.float32)
    for i, s in enumerate(uniq_styles):
        mask[i, styles == s] = 1

    # Precompute return matrix once
    port_np = rtn_df.to_numpy(dtype=np.float32)
    ann_exp = 12 / T

    # ------------------------------------------------------------------
    # Batch processing for memory efficiency
    # ------------------------------------------------------------------
    best_cagr = -np.inf
    best_mdd = -np.inf
    best_rank = np.inf
    best_raw_weights = None
    best_fitted_weights = None

    all_cagrs = []
    all_mdds = []
    all_raw_weights = []
    all_fitted_weights = []

    n_batches = (num_sims + batch_size - 1) // batch_size

    for batch_idx in range(n_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, num_sims)
        current_batch_size = end_idx - start_idx

        # ------------------------------------------------------------------
        # 1. Random raw weights (Σ w = 1 per column) - float32
        # ------------------------------------------------------------------
        raw_mat = np.random.rand(K, current_batch_size).astype(np.float32)
        raw_mat /= raw_mat.sum(axis=0, keepdims=True)

        # ------------------------------------------------------------------
        # 3. Apply style caps (shrink & redistribute)
        # ------------------------------------------------------------------
        share = mask @ raw_mat
        excess = np.clip(share - style_cap, a_min=0, a_max=None)
        scale = np.where(share > style_cap, style_cap / share, 1.0).astype(np.float32)
        shrink = mask.T @ scale
        mat_scaled = raw_mat * shrink

        room = np.where(share < style_cap, style_cap - share, 0).astype(np.float32)
        room_sum = room.sum(axis=0, keepdims=True)
        ratio = np.divide(room, room_sum, out=np.zeros_like(room), where=room_sum != 0)
        add = excess.sum(axis=0, keepdims=True) * ratio
        fitted_mat = mat_scaled + (mask.T @ add)
        fitted_mat /= fitted_mat.sum(axis=0, keepdims=True)

        # Feasibility filter
        ok = (mask @ fitted_mat <= style_cap + tol).all(axis=0)
        raw_mat_ok = raw_mat[:, ok]
        fitted_mat_ok = fitted_mat[:, ok]

        if fitted_mat_ok.shape[1] == 0:
            continue

        # ------------------------------------------------------------------
        # 4. Simulate returns - vectorized NumPy operations
        # ------------------------------------------------------------------
        sim = port_np @ fitted_mat_ok  # (T × sims)

        # Vectorized cumulative product and metrics
        cum = np.cumprod(1 + sim, axis=0)
        cagr_batch = np.power(cum[-1, :], ann_exp) - 1

        # Vectorized MDD calculation
        running_max = np.maximum.accumulate(cum, axis=0)
        drawdown = cum / running_max - 1
        mdd_batch = drawdown.min(axis=0)

        # Store results
        all_cagrs.append(cagr_batch)
        all_mdds.append(mdd_batch)
        all_raw_weights.append(raw_mat_ok)
        all_fitted_weights.append(fitted_mat_ok)

    # ------------------------------------------------------------------
    # 5. Combine all batches and find best portfolio
    # ------------------------------------------------------------------
    if len(all_cagrs) == 0:
        raise ValueError("No feasible portfolios found")

    all_cagrs = np.concatenate(all_cagrs)
    all_mdds = np.concatenate(all_mdds)
    all_raw_weights = np.concatenate(all_raw_weights, axis=1)
    all_fitted_weights = np.concatenate(all_fitted_weights, axis=1)

    # Calculate ranks
    rank_cagr = np.argsort(np.argsort(-all_cagrs)) + 1  # descending rank
    rank_mdd = np.argsort(np.argsort(-all_mdds)) + 1    # descending rank (less negative is better)
    rank_total = rank_cagr * 0.6 + rank_mdd * 0.4

    best_idx = np.argmin(rank_total)

    best_stats = pd.DataFrame({
        "cagr": [all_cagrs[best_idx]],
        "mdd": [all_mdds[best_idx]],
        "rank_cagr": [float(rank_cagr[best_idx])],
        "rank_mdd": [float(rank_mdd[best_idx])],
        "rank_total": [float(rank_total[best_idx])]
    })

    factors = rtn_df.columns.to_numpy()

    weights_tbl = pd.DataFrame({
        "factor": factors,
        "raw_weight": all_raw_weights[:, best_idx],
        "styleName": styles,
        "fitted_weight": all_fitted_weights[:, best_idx],
    })

    weights_tbl = (
        weights_tbl[weights_tbl["raw_weight"] > 0]
        .sort_values("raw_weight", ascending=False)
        .reset_index(drop=True)
    )

    logger.info(f"[Trace] Simulation completed. Best stats: {best_stats.to_dict('records')}")
    return best_stats, weights_tbl


def run_model_portfolio_pipeline(start_date, end_date, report: bool = False, test_file: str | None = None) -> None:
    """모델 포트폴리오 생성 파이프라인 전체 실행

    【목적】
    - ETL → 최적화 → 출력의 전체 프로세스 조율
    - README.md의 전체 단계 [1️⃣~4️⃣]를 순차적으로 실행

    【비유】
    - 오케스트라 지휘자 역할
    - 각 악기(함수)들을 적절한 타이밍에 연주시킴

    【README.md 연결】
    - [1️⃣] 팩터 데이터베이스 구축
    - [2️⃣] 팩터 후보군 선정
    - [3️⃣] Model Portfolio(MP) 구성
    - [4️⃣] 결과물 산출
    """

    # ═══════════════════════════════════════════════════════════════════════════
    # [1️⃣] 팩터 데이터베이스 구축 - 데이터 로딩
    # ═══════════════════════════════════════════════════════════════════════════
    # parquet 파일 또는 테스트 CSV 파일 로드하기
    t0 = time.time()
    if test_file:
        import re
        test_data_path = Path.cwd() / test_file
        raw_factor_data_df = pd.read_csv(test_data_path)
        # fld 컬럼에서 factorAbbreviation 추출 (예: "Sales Acceleration (SalesAcc)" -> "SalesAcc")
        def extract_abbr(fld_value):
            match = re.search(r'\(([^)]+)\)$', fld_value)
            return match.group(1) if match else fld_value
        raw_factor_data_df['factorAbbreviation'] = raw_factor_data_df['fld'].apply(extract_abbr)
        raw_factor_data_df = raw_factor_data_df.drop(columns=['fld', 'updated_at'])

        # CSV 파일의 ddt 컬럼에서 날짜 범위 추출
        raw_factor_data_df['ddt'] = pd.to_datetime(raw_factor_data_df['ddt'])
        start_date = raw_factor_data_df['ddt'].min().strftime('%Y-%m-%d')
        end_date = raw_factor_data_df['ddt'].max().strftime('%Y-%m-%d')

        logger.info(f"Query loaded from {test_data_path} in {time.time() - t0:.2f}s")
        logger.info(f"[Trace] Loaded test data. Shape: {raw_factor_data_df.shape}")
        logger.info(f"[Trace] Extracted date range: {start_date} to {end_date}")
    else:
        parquet_path = DATA_DIR / f"{PARAM['benchmark']}_{start_date}_{end_date}.parquet"
        raw_factor_data_df = pd.read_parquet(parquet_path)
        logger.info(f"Query loaded from {parquet_path} in {time.time() - t0:.2f}s")
        logger.info(f"[Trace] Loaded parquet data. Shape: {raw_factor_data_df.shape}")

    # ───────────────────────────────────────────────────────────────────────────
    # 메타데이터(순서/스타일/이름)와 조인
    # ───────────────────────────────────────────────────────────────────────────
    t1 = time.time()
    factor_metadata_df = pd.read_csv(DATA_DIR / "factor_info.csv")
    merged_factor_data_df = raw_factor_data_df.merge(factor_metadata_df, on="factorAbbreviation", how="inner")
    logger.info(f"[Trace] Merged with factor info. Shape: {merged_factor_data_df.shape}")

    factor_abbr_list, orders = factor_metadata_df.factorAbbreviation.tolist(), factor_metadata_df.factorOrder.tolist()

    # ═══════════════════════════════════════════════════════════════════════════
    # [2️⃣-1] 백테스트 기간 설정 (construct_long_short_df)
    # [2️⃣-2] 팩터별 5분위(Quintile) 포트폴리오 구성
    # ═══════════════════════════════════════════════════════════════════════════
    # calculate_factor_stats() 호출
    # 최적화: M_RETURN 미리 추출
    market_return_df = (
        raw_factor_data_df[raw_factor_data_df["factorAbbreviation"] == "M_RETURN"].reset_index(drop=True)
        .rename(columns={"val": "M_RETURN"})
        .drop(columns=["factorAbbreviation"])
    )
    logger.info(f"[Trace] Extracted M_RETURN. DDT distinct: {market_return_df['ddt'].nunique()}, GVKeyIID distinct: {market_return_df['gvkeyiid'].nunique()}")

    # 최적화: M_RETURN을 전체 데이터에 병합 (Loop 밖에서 수행)
    # 이를 통해 calculate_factor_stats 내부의 반복적인 병합을 제거함
    merged_factor_data_df = (
        merged_factor_data_df
        .merge(
            market_return_df,
            on=["gvkeyiid", "ticker", "isin", "ddt", "sec", "country"],
            how="inner",
        )
        .query("sec != 'Undefined'")  # 정의되지 않은 섹터 전역 필터링
    )
    logger.info(f"[Trace] Merged M_RETURN globally. Shape: {merged_factor_data_df.shape}")

    # 최적화: meta를 미리 그룹화
    grouped_source_data = merged_factor_data_df.groupby("factorAbbreviation")
    logger.info(f"[Trace] Grouped source data. Number of groups: {grouped_source_data.ngroups}")

    processed_factor_data_list: List[Any] = []
    for factor_abbr, order in track(zip(factor_abbr_list, orders), total=len(factor_abbr_list), description="Assigning factors"):
        # 그룹이 존재하면 가져오고, 없으면 빈 DataFrame 전달
        if factor_abbr in grouped_source_data.groups:
            factor_data_df = grouped_source_data.get_group(factor_abbr).copy()  # copy to avoid SettingWithCopy
        else:
            factor_data_df = pd.DataFrame(columns=merged_factor_data_df.columns)

        # market_return_df 인자 제거 (이미 병합됨)
        processed_factor_data_list.append(calculate_factor_stats(factor_abbr, order, factor_data_df, test_mode=bool(test_file)))
    logger.info(f"Factors assigned in {time.time() - t1:.2f}s")

    """Run the full ETL → optimisation → export process."""
    logger.info("Report generation started for period: %s to %s", start_date, end_date)

    # ───────────────────────────────────────────────────────────────────────────
    # 변수 준비
    # ───────────────────────────────────────────────────────────────────────────
    factor_abbr_list, factor_name_list, style_name_list, raw = factor_abbr_list, factor_metadata_df.factorName.tolist(), factor_metadata_df.styleName.tolist(), processed_factor_data_list

    if report:
        from service.report.read_pkl import generate_report
        import sys
        logger.info("Report generation requested. Invoking read_pkl logic...")
        generate_report(factor_abbr_list, factor_name_list, style_name_list, raw)
        logger.info("Report generated. Exiting.")
        sys.exit(0)

    # ═══════════════════════════════════════════════════════════════════════════
    # [2️⃣-3] 비투자 섹터 결정 (filter_and_label_factors)
    # [2️⃣-4] 투자 대상 분위(롱/숏) 결정 (filter_and_label_factors)
    # ═══════════════════════════════════════════════════════════════════════════
    # filter_and_label_factors() 호출
    kept_factor_abbrs, kept_name, kept_style, _, _, filtered_factor_data_list = filter_and_label_factors(factor_abbr_list, factor_name_list, style_name_list, raw)

    # ═══════════════════════════════════════════════════════════════════════════
    # [2️⃣-5] 팩터 스프레드 수익률 측정 (construct_long_short_df, calculate_vectorized_return, aggregate_factor_returns)
    # [2️⃣-6] 팩터 후보군 최종 선정 (evaluate_factor_universe)
    # ═══════════════════════════════════════════════════════════════════════════
    # evaluate_factor_universe() 호출
    monthly_return_matrix, downside_correlation_matrix, factor_performance_metrics = evaluate_factor_universe(kept_factor_abbrs, kept_name, kept_style, filtered_factor_data_list, test_file)

    # ═══════════════════════════════════════════════════════════════════════════
    # [3️⃣-1] 스타일별 최상위 팩터 선정 (여기서 직접 실행)
    # [3️⃣-2] 보완 팩터 선정 및 2-팩터 믹스 (find_optimal_mix)
    # ═══════════════════════════════════════════════════════════════════════════
    top_metrics = factor_performance_metrics.groupby("styleName", as_index=False).first()  # 스타일별 최상위 팩터
    grids = []
    for _, row in top_metrics.iterrows():  # .iterrows() 가 인덱스, 값으로 반환
        grid, *_ = find_optimal_mix(monthly_return_matrix, row.to_frame().T.reset_index(drop=True), downside_correlation_matrix)
        grid["styleName"] = row["styleName"]
        grids.append(grid)
    mix_grid = pd.concat(grids, ignore_index=True)

    # ───────────────────────────────────────────────────────────────────────────
    # 최적 서브 팩터 선택 및 수익률 행렬 부분집합 생성
    # ───────────────────────────────────────────────────────────────────────────
    best_sub = (
        mix_grid.sort_values("rank_total")  # ascending by rank_total
        .groupby("main_factor", as_index=False)  # group by each main_factor
        .first()[["main_factor", "sub_factor"]]  # keep the smallest-rank row
    )

    # ------------------------------------------------------------------
    # Map factor → style and append to best_sub
    # ------------------------------------------------------------------
    style_map = factor_performance_metrics.set_index("factorAbbreviation")["styleName"]
    best_sub["main_style"] = best_sub["main_factor"].map(style_map)
    best_sub["sub_style"] = best_sub["sub_factor"].map(style_map)
    best_sub = best_sub[["main_factor", "main_style", "sub_factor", "sub_style"]]

    # Save if needed
    # best_sub.to_csv(OUTPUT_DIR / "best_sub_factor.csv", index=False)

    # 6. 메인 팩터와 보조 팩터로 수익률 행렬 합집합 생성
    cols_to_keep = pd.unique(best_sub[["main_factor", "sub_factor"]].to_numpy().ravel())
    ret_subset = monthly_return_matrix[cols_to_keep]

    # ───────────────────────────────────────────────────────────────────────────
    # factor_list 및 style_list 구성 (정렬된 순서)
    # ───────────────────────────────────────────────────────────────────────────
    factor_list = pd.unique(best_sub[["main_factor", "sub_factor"]].to_numpy().ravel()).tolist()
    style_list = [style_map[f] for f in factor_list]

    # ═══════════════════════════════════════════════════════════════════════════
    # [3️⃣-3] 스타일 제약 하 최적 비중 결정 (simulate_constrained_weights)
    # ═══════════════════════════════════════════════════════════════════════════
    sim_result = simulate_constrained_weights(ret_subset, style_list, test_mode=bool(test_file))

    # ═══════════════════════════════════════════════════════════════════════════
    # [4️⃣-1] 종목별 최종 비중 산출 (여기서 직접 실행)
    # ═══════════════════════════════════════════════════════════════════════════
    # 팩터 인덱스 맵 생성 (list.index() 반복 호출 제거)
    factor_idx_map = {fac: idx for idx, fac in enumerate(kept_factor_abbrs)}

    # sim_result[1]을 딕셔너리로 변환하여 반복 접근 최적화
    sim_factors = sim_result[1][['factor', 'fitted_weight', 'styleName']].to_dict('records')

    weight_frames = []
    for row in sim_factors:
        fac = row['factor']
        w = row['fitted_weight']
        s = row['styleName']

        j = factor_idx_map[fac]
        df = filtered_factor_data_list[j][['ddt', 'ticker', 'isin', 'gvkeyiid', 'label']].copy()

        # groupby transform 한 번만 호출하고 재사용
        count_per_group = df.groupby(['ddt', 'label'])['label'].transform('count')

        df['weight'] = df['label'] * w / count_per_group
        df['ls_weight'] = df['label'] / count_per_group
        df['factor_weight'] = w
        df['style'] = s
        df['name'] = f'MXCN1A_{s}'
        df['factor'] = fac
        df['count'] = count_per_group
        df["ticker"] = df["ticker"].astype(str).str.zfill(6).add(" CH Equity")

        # end_date 필터링 후 필요한 컬럼만 선택
        end_date_df = df.loc[df['ddt'] == end_date, ['ddt', 'ticker', 'isin', 'gvkeyiid', 'weight',
                                                      'ls_weight', 'factor_weight',
                                                      'factor', 'style', 'name', 'count']].reset_index(drop=True)
        weight_frames.append(end_date_df)

    # ═══════════════════════════════════════════════════════════════════════════
    # [4️⃣-2] Model Portfolio(MP) 구성 (여기서 직접 실행)
    # ═══════════════════════════════════════════════════════════════════════════
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
    agg_w['count'] = (
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

    # ═══════════════════════════════════════════════════════════════════════════
    # [4️⃣-3] 결과물 산출 (여기서 직접 실행)
    # ═══════════════════════════════════════════════════════════════════════════
    suffix = f"_{Path(test_file).stem}" if test_file else ""
    agg_w.to_csv(OUTPUT_DIR / f"aggregated_weights_{end_date}_test{suffix}.csv")
    final_weights.to_csv(OUTPUT_DIR / f"total_aggregated_weights_{end_date}_test{suffix}.csv")

    final_style_weight.to_csv(OUTPUT_DIR / f"total_aggregated_weights_style_{end_date}_test{suffix}.csv")

    final_weights.loc[final_weights['style'] == 'MP', 'factor_weight'] = 1
    final_weights = final_weights.replace(0, np.nan)

    pivoted_final = final_weights.pivot_table(
        index=['ddt', 'ticker', 'isin', 'gvkeyiid'],
        columns=['style', 'factor_weight', 'factor'],
        values='weight',
        aggfunc='sum'
    ).reset_index()

    sample_df = pd.DataFrame({"factor": pivoted_final.columns.get_level_values(2).tolist()[4:]})
    sum_df = pd.merge(sim_result[1], sample_df, on='factor', how='inner')

    final_weights.loc[final_weights['style'] == 'MP', 'factor_weight'] = sum_df['fitted_weight'].sum(axis=0)
    final_weights = final_weights.replace(0, np.nan)

    pivoted_final = final_weights.pivot_table(
        index=['ddt', 'ticker', 'isin', 'gvkeyiid'],
        columns=['style', 'factor_weight', 'factor'],
        values='weight',
        aggfunc='sum'
    ).reset_index()

    cols = pivoted_final.columns
    mp_mask = cols.get_level_values('style') == 'MP'

    new_order = cols[~mp_mask].tolist() + cols[mp_mask].tolist()
    pivoted_final = pivoted_final.loc[:, new_order]

    pivoted_final.to_csv(OUTPUT_DIR / f"pivoted_total_agg_wgt_{end_date}{suffix}.csv")
    logger.info("Pipeline completed ✓ — files saved in %s", OUTPUT_DIR)