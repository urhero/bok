# -*- coding: utf-8 -*-
"""
Unit tests for prepend_start_zero() function.

prepend_start_zero() 함수 테스트:
- 시계열 데이터 맨 앞에 0을 추가하는 함수
- 누적 수익률 계산의 기준선(출발점) 역할
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pandas import DateOffset

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from service.live.model_portfolio import prepend_start_zero


class TestPrependStartZero:
    """prepend_start_zero 함수 테스트 클래스"""

    def test_basic_functionality(self, sample_time_series: pd.DataFrame) -> None:
        """기본 기능 테스트: 시계열 앞에 0이 추가되는지 확인"""
        result = prepend_start_zero(sample_time_series.copy())

        # 원본보다 1행 더 많아야 함
        assert len(result) == len(sample_time_series) + 1

        # 첫 번째 값이 0이어야 함
        assert result.iloc[0].values[0] == 0.0

        # 정렬되어 있어야 함 (날짜 오름차순)
        assert result.index.is_monotonic_increasing

    def test_first_date_is_one_month_before(self, sample_time_series: pd.DataFrame) -> None:
        """첫 번째 날짜가 원본의 첫 날짜보다 1개월 전인지 확인"""
        original_first_date = sample_time_series.index[0]
        result = prepend_start_zero(sample_time_series.copy())

        expected_first_date = original_first_date - DateOffset(months=1)
        assert result.index[0] == expected_first_date

    def test_original_values_preserved(self, sample_time_series: pd.DataFrame) -> None:
        """원본 데이터가 보존되는지 확인"""
        original_values = sample_time_series.values.copy()
        result = prepend_start_zero(sample_time_series.copy())

        # 원본 값들이 결과에 포함되어 있어야 함
        np.testing.assert_array_almost_equal(
            result.iloc[1:].values,
            original_values
        )

    def test_single_value_series(self, single_value_time_series: pd.DataFrame) -> None:
        """단일 값 시계열에서도 동작하는지 확인"""
        result = prepend_start_zero(single_value_time_series.copy())

        assert len(result) == 2
        assert result.iloc[0].values[0] == 0.0

    def test_column_names_preserved(self, sample_time_series: pd.DataFrame) -> None:
        """컬럼 이름이 보존되는지 확인"""
        original_columns = sample_time_series.columns.tolist()
        result = prepend_start_zero(sample_time_series.copy())

        assert result.columns.tolist() == original_columns

    def test_multi_column_dataframe(self) -> None:
        """여러 컬럼이 있는 DataFrame에서도 동작하는지 확인"""
        dates = pd.date_range("2024-01-31", periods=3, freq="ME")
        df = pd.DataFrame({
            "factor_A": [0.05, 0.03, -0.02],
            "factor_B": [0.02, -0.01, 0.04],
        }, index=dates)

        result = prepend_start_zero(df.copy())

        # 모든 컬럼의 첫 번째 값이 0이어야 함
        assert len(result) == 4
        assert (result.iloc[0] == 0.0).all()

    def test_index_dtype_preserved(self, sample_time_series: pd.DataFrame) -> None:
        """인덱스 데이터 타입이 유지되는지 확인"""
        result = prepend_start_zero(sample_time_series.copy())

        # DatetimeIndex 타입 유지
        assert isinstance(result.index, pd.DatetimeIndex)

    def test_unsorted_input(self) -> None:
        """정렬되지 않은 입력도 정렬된 결과를 반환하는지 확인"""
        dates = pd.to_datetime(["2024-03-31", "2024-01-31", "2024-02-29"])
        df = pd.DataFrame({"factor_A": [0.01, 0.02, 0.03]}, index=dates)

        result = prepend_start_zero(df.copy())

        # 정렬되어 있어야 함
        assert result.index.is_monotonic_increasing

    def test_with_negative_values(self) -> None:
        """음수 값이 포함된 시계열 테스트"""
        dates = pd.date_range("2024-01-31", periods=3, freq="ME")
        df = pd.DataFrame({"factor_A": [-0.05, -0.03, -0.02]}, index=dates)

        result = prepend_start_zero(df.copy())

        # 첫 번째 값은 0이고, 원본 음수 값들은 유지
        assert result.iloc[0].values[0] == 0.0
        assert (result.iloc[1:].values < 0).all()

    def test_cumulative_return_calculation(self) -> None:
        """누적 수익률 계산에 적합한 형태인지 확인

        prepend_start_zero의 목적: 누적 수익률 계산 시 시작점을 0%로 만듦
        (1 + 0) * (1 + r1) * (1 + r2) ... 형태로 계산 가능해야 함
        """
        dates = pd.date_range("2024-01-31", periods=3, freq="ME")
        df = pd.DataFrame({"returns": [0.10, 0.05, -0.03]}, index=dates)

        result = prepend_start_zero(df.copy())

        # 누적 수익률 계산
        cumulative = (1 + result["returns"]).cumprod()

        # 첫 번째 값은 1.0 (0% 수익률)
        assert cumulative.iloc[0] == 1.0

        # 마지막 누적 수익률 확인
        expected_final = (1 + 0) * (1 + 0.10) * (1 + 0.05) * (1 - 0.03)
        np.testing.assert_almost_equal(cumulative.iloc[-1], expected_final)


class TestPrependStartZeroEdgeCases:
    """prepend_start_zero 엣지 케이스 테스트"""

    def test_with_nan_values(self) -> None:
        """NaN 값이 포함된 시계열 테스트"""
        dates = pd.date_range("2024-01-31", periods=3, freq="ME")
        df = pd.DataFrame({"factor_A": [0.05, np.nan, -0.02]}, index=dates)

        result = prepend_start_zero(df.copy())

        # 첫 번째 값은 0, NaN은 유지
        assert result.iloc[0].values[0] == 0.0
        assert pd.isna(result.iloc[2].values[0])

    def test_with_inf_values(self) -> None:
        """Inf 값이 포함된 시계열 테스트"""
        dates = pd.date_range("2024-01-31", periods=3, freq="ME")
        df = pd.DataFrame({"factor_A": [0.05, np.inf, -0.02]}, index=dates)

        result = prepend_start_zero(df.copy())

        assert result.iloc[0].values[0] == 0.0
        assert np.isinf(result.iloc[2].values[0])

    def test_month_end_dates(self) -> None:
        """월말 날짜 처리 테스트 (2월 28일/29일 등)"""
        # 2024년은 윤년이므로 2월 29일
        dates = pd.to_datetime(["2024-02-29", "2024-03-31", "2024-04-30"])
        df = pd.DataFrame({"factor_A": [0.05, 0.03, -0.02]}, index=dates)

        result = prepend_start_zero(df.copy())

        # 2024-02-29의 한 달 전은 2024-01-29
        expected_first_date = pd.Timestamp("2024-01-29")
        assert result.index[0] == expected_first_date


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
