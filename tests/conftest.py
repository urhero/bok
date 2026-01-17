# -*- coding: utf-8 -*-
"""
Shared pytest fixtures for Awesome-Cohen (BoK) tests.

이 파일에는 테스트에서 공통으로 사용하는 데이터와 설정이 포함됩니다.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pathlib import Path


# ═══════════════════════════════════════════════════════════════════════════════
# 경로 설정
# ═══════════════════════════════════════════════════════════════════════════════
PROJECT_ROOT = Path(__file__).parent.parent
TEST_DATA_PATH = PROJECT_ROOT / "test_data.csv"
FACTOR_INFO_PATH = PROJECT_ROOT / "factor_info.csv"


# ═══════════════════════════════════════════════════════════════════════════════
# 기본 데이터 Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def sample_time_series() -> pd.DataFrame:
    """시계열 테스트용 샘플 데이터 (prepend_start_zero 테스트용)"""
    dates = pd.date_range("2024-01-31", periods=3, freq="ME")
    return pd.DataFrame(
        {"factor_A": [0.05, 0.03, -0.02]},
        index=dates
    )


@pytest.fixture
def empty_time_series() -> pd.DataFrame:
    """빈 시계열 데이터 (엣지 케이스 테스트용)"""
    return pd.DataFrame({"factor_A": []}, index=pd.DatetimeIndex([]))


@pytest.fixture
def single_value_time_series() -> pd.DataFrame:
    """단일 값 시계열 데이터 (엣지 케이스 테스트용)"""
    dates = pd.date_range("2024-01-31", periods=1, freq="ME")
    return pd.DataFrame({"factor_A": [0.05]}, index=dates)


# ═══════════════════════════════════════════════════════════════════════════════
# Factor 데이터 Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def sample_factor_data() -> pd.DataFrame:
    """calculate_factor_stats 테스트용 팩터 데이터

    최소 요구사항:
    - 3개 이상의 날짜 (2개 이하면 스킵됨)
    - 섹터별 종목들
    - val(팩터값), M_RETURN(수익률), factorAbbreviation 컬럼
    """
    np.random.seed(42)

    # 4개 날짜, 2개 섹터, 섹터당 15개 종목
    dates = pd.date_range("2024-01-31", periods=4, freq="ME")
    sectors = ["IT", "Finance"]
    tickers_per_sector = 15

    rows = []
    for date in dates:
        for sector in sectors:
            for i in range(tickers_per_sector):
                ticker = f"{sector}_{i:03d}"
                rows.append({
                    "gvkeyiid": f"GV{sector}{i:03d}",
                    "ticker": ticker,
                    "isin": f"KR{sector}{i:06d}",
                    "ddt": date,
                    "sec": sector,
                    "country": "KR",
                    "factorAbbreviation": "TEST_FACTOR",
                    "val": np.random.randn() * 10 + 50,  # 팩터값
                    "M_RETURN": np.random.randn() * 0.05,  # 월간 수익률 (-5% ~ +5%)
                })

    return pd.DataFrame(rows)


@pytest.fixture
def insufficient_history_data() -> pd.DataFrame:
    """날짜가 2개 이하인 데이터 (스킵되어야 함)"""
    dates = pd.date_range("2024-01-31", periods=2, freq="ME")

    rows = []
    for date in dates:
        for i in range(10):
            rows.append({
                "gvkeyiid": f"GV{i:03d}",
                "ticker": f"TICK_{i:03d}",
                "isin": f"KR{i:06d}",
                "ddt": date,
                "sec": "IT",
                "country": "KR",
                "factorAbbreviation": "TEST_FACTOR",
                "val": np.random.randn() * 10 + 50,
                "M_RETURN": np.random.randn() * 0.05,
            })

    return pd.DataFrame(rows)


@pytest.fixture
def small_sector_data() -> pd.DataFrame:
    """섹터당 종목수가 10개 이하인 데이터 (test_mode가 아니면 NaN 처리됨)"""
    np.random.seed(42)
    dates = pd.date_range("2024-01-31", periods=4, freq="ME")

    rows = []
    for date in dates:
        for i in range(8):  # 8개 종목만 (10개 이하)
            rows.append({
                "gvkeyiid": f"GV{i:03d}",
                "ticker": f"TICK_{i:03d}",
                "isin": f"KR{i:06d}",
                "ddt": date,
                "sec": "SmallSector",
                "country": "KR",
                "factorAbbreviation": "TEST_FACTOR",
                "val": np.random.randn() * 10 + 50,
                "M_RETURN": np.random.randn() * 0.05,
            })

    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════════════
# 상관관계 테스트용 Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def sample_return_matrix() -> pd.DataFrame:
    """calculate_downside_correlation 테스트용 수익률 행렬"""
    np.random.seed(42)
    dates = pd.date_range("2020-01-31", periods=60, freq="ME")  # 5년 월간 데이터

    # 3개 팩터, 일부 상관관계 있는 수익률 생성
    base = np.random.randn(60) * 0.03

    return pd.DataFrame({
        "Factor_A": base + np.random.randn(60) * 0.01,
        "Factor_B": base * 0.5 + np.random.randn(60) * 0.02,  # A와 양의 상관
        "Factor_C": -base * 0.3 + np.random.randn(60) * 0.025,  # A와 음의 상관
    }, index=dates)


@pytest.fixture
def all_positive_returns() -> pd.DataFrame:
    """모든 수익률이 양수인 데이터 (하락 상관관계 계산 불가)"""
    np.random.seed(42)
    dates = pd.date_range("2020-01-31", periods=30, freq="ME")

    return pd.DataFrame({
        "Factor_A": np.abs(np.random.randn(30) * 0.03) + 0.01,  # 모두 양수
        "Factor_B": np.abs(np.random.randn(30) * 0.02) + 0.01,
    }, index=dates)


# ═══════════════════════════════════════════════════════════════════════════════
# 시뮬레이션 테스트용 Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def sample_style_returns() -> tuple[pd.DataFrame, list[str]]:
    """simulate_constrained_weights 테스트용 스타일 수익률"""
    np.random.seed(42)
    dates = pd.date_range("2020-01-31", periods=36, freq="ME")  # 3년

    rtn_df = pd.DataFrame({
        "val_factor_1": np.random.randn(36) * 0.03,
        "val_factor_2": np.random.randn(36) * 0.025,
        "mom_factor_1": np.random.randn(36) * 0.04,
        "mom_factor_2": np.random.randn(36) * 0.035,
        "qual_factor_1": np.random.randn(36) * 0.02,
    }, index=dates)

    style_list = ["Valuation", "Valuation", "Momentum", "Momentum", "Quality"]

    return rtn_df, style_list


# ═══════════════════════════════════════════════════════════════════════════════
# 실제 테스트 데이터 Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def test_data_csv() -> pd.DataFrame | None:
    """실제 test_data.csv 파일 로드 (파일이 있는 경우에만)"""
    if TEST_DATA_PATH.exists():
        return pd.read_csv(TEST_DATA_PATH, parse_dates=["ddt"])
    return None


@pytest.fixture
def factor_info_csv() -> pd.DataFrame | None:
    """실제 factor_info.csv 파일 로드 (파일이 있는 경우에만)"""
    if FACTOR_INFO_PATH.exists():
        return pd.read_csv(FACTOR_INFO_PATH)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Filter/Label 테스트용 Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def sample_sector_return_df() -> pd.DataFrame:
    """filter_and_label_factors 테스트용 섹터 수익률"""
    return pd.DataFrame(
        {
            "IT": [0.05, 0.04, 0.02, -0.01, -0.03],      # Q1-Q5 spread = 0.08 (양수)
            "Finance": [0.02, 0.01, 0.00, -0.02, 0.03],  # Q1-Q5 spread = -0.01 (음수, 제거대상)
            "Health": [0.06, 0.05, 0.03, 0.01, -0.02],   # Q1-Q5 spread = 0.08 (양수)
        },
        index=["Q1", "Q2", "Q3", "Q4", "Q5"]
    ).T  # 전치하여 섹터가 행, 분위가 컬럼


@pytest.fixture
def sample_raw_df_for_filter() -> pd.DataFrame:
    """filter_and_label_factors 테스트용 종목 데이터"""
    np.random.seed(42)
    dates = pd.date_range("2024-01-31", periods=3, freq="ME")
    sectors = ["IT", "Finance", "Health"]
    quantiles = ["Q1", "Q2", "Q3", "Q4", "Q5"]

    rows = []
    for date in dates:
        for sector in sectors:
            for i, q in enumerate(quantiles):
                for j in range(5):  # 분위당 5개 종목
                    rows.append({
                        "gvkeyiid": f"GV{sector[:2]}{i}{j}",
                        "ticker": f"{sector[:2]}_{i}_{j}",
                        "isin": f"KR{sector[:2]}{i}{j:04d}",
                        "ddt": date,
                        "sec": sector,
                        "country": "KR",
                        "quantile": q,
                        "M_RETURN": np.random.randn() * 0.05,
                        "TEST_FACTOR": np.random.randn() * 10 + 50,
                    })

    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════════════
# 유틸리티 함수
# ═══════════════════════════════════════════════════════════════════════════════

def assert_dataframe_equal_with_tolerance(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    rtol: float = 1e-5,
    atol: float = 1e-8,
) -> None:
    """DataFrame 비교 (수치 오차 허용)

    Parameters
    ----------
    actual : pd.DataFrame
        실제 결과
    expected : pd.DataFrame
        예상 결과
    rtol : float
        상대 오차 허용치
    atol : float
        절대 오차 허용치
    """
    pd.testing.assert_frame_equal(
        actual,
        expected,
        rtol=rtol,
        atol=atol,
        check_exact=False,
    )


def assert_weights_valid(weights: pd.Series | np.ndarray, tolerance: float = 0.01) -> None:
    """포트폴리오 가중치 유효성 검사

    Parameters
    ----------
    weights : pd.Series | np.ndarray
        포트폴리오 가중치
    tolerance : float
        가중치 합계 허용 오차 (기본 1%)
    """
    weights_array = np.asarray(weights)

    # 가중치가 NaN이 아닌지
    assert not np.any(np.isnan(weights_array)), "Weights contain NaN values"

    # 가중치가 음수가 아닌지 (롱-숏이 아닌 경우)
    # Note: 롱-숏 포트폴리오는 음수 가중치 허용

    # 가중치 합이 약 1인지 (롱 온리 포트폴리오의 경우)
    weight_sum = np.sum(np.abs(weights_array))
    if weight_sum > 0:  # 빈 포트폴리오가 아닌 경우만 체크
        assert abs(weight_sum - 1.0) < tolerance or weight_sum <= tolerance, \
            f"Weight sum {weight_sum} is not close to 1.0"
