# -*- coding: utf-8 -*-
"""construct_long_short_df 및 calculate_vectorized_return 함수 유닛 테스트.

롱/숏 분리, 동일가중 비중 부여, 포트폴리오 수익률 계산을 검증한다.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from service.pipeline.weight_construction import construct_long_short_df, calculate_vectorized_return


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def basic_labeled_data() -> pd.DataFrame:
    """기본 라벨링 데이터: 2개 날짜, 롱/숏/뉴트럴 혼합."""
    return pd.DataFrame({
        "ddt": pd.to_datetime([
            "2024-01-31", "2024-01-31", "2024-01-31", "2024-01-31",
            "2024-02-29", "2024-02-29", "2024-02-29", "2024-02-29",
        ]),
        "gvkeyiid": ["A", "B", "C", "D", "A", "B", "C", "D"],
        "ticker": ["T_A", "T_B", "T_C", "T_D", "T_A", "T_B", "T_C", "T_D"],
        "M_RETURN": [0.03, -0.01, 0.02, -0.02, 0.01, 0.04, -0.03, 0.00],
        "label": [1, -1, 0, 1, 1, -1, 0, 1],
    })


@pytest.fixture
def labeled_data_with_early_dates() -> pd.DataFrame:
    """2017-12-31 이전 데이터를 포함하는 라벨링 데이터."""
    return pd.DataFrame({
        "ddt": pd.to_datetime([
            "2017-11-30", "2017-11-30",  # 제외 대상
            "2017-12-31", "2017-12-31",  # 경계값 (포함)
            "2018-01-31", "2018-01-31",  # 포함
        ]),
        "gvkeyiid": ["A", "B", "A", "B", "A", "B"],
        "ticker": ["T_A", "T_B", "T_A", "T_B", "T_A", "T_B"],
        "M_RETURN": [0.05, -0.03, 0.02, -0.01, 0.04, -0.02],
        "label": [1, -1, 1, -1, 1, -1],
    })


@pytest.fixture
def single_date_portfolio() -> pd.DataFrame:
    """단일 날짜 포트폴리오 데이터 (calculate_vectorized_return용)."""
    return pd.DataFrame({
        "ddt": pd.to_datetime(["2024-01-31", "2024-01-31"]),
        "gvkeyiid": ["A", "B"],
        "ticker": ["T_A", "T_B"],
        "M_RETURN": [0.03, 0.01],
        "label": [1, 1],
        "signal": ["L", "L"],
        "num": [2, 2],
        "return_weight": [0.5, 0.5],
        "turnover_weight": [0.5, 0.5],
    })


@pytest.fixture
def multi_date_portfolio() -> pd.DataFrame:
    """다중 날짜 포트폴리오 데이터 (calculate_vectorized_return용)."""
    return pd.DataFrame({
        "ddt": pd.to_datetime([
            "2024-01-31", "2024-01-31",
            "2024-02-29", "2024-02-29",
            "2024-03-31", "2024-03-31",
        ]),
        "gvkeyiid": ["A", "B", "A", "B", "A", "B"],
        "ticker": ["T_A", "T_B", "T_A", "T_B", "T_A", "T_B"],
        "M_RETURN": [0.03, 0.01, 0.02, -0.01, 0.04, 0.02],
        "label": [1, 1, 1, 1, 1, 1],
        "signal": ["L", "L", "L", "L", "L", "L"],
        "num": [2, 2, 2, 2, 2, 2],
        "return_weight": [0.5, 0.5, 0.5, 0.5, 0.5, 0.5],
        "turnover_weight": [0.5, 0.5, 0.5, 0.5, 0.5, 0.5],
    })


# ═══════════════════════════════════════════════════════════════════════════════
# construct_long_short_df Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestConstructLongShortDfBasic:
    """기본 롱/숏 분리 테스트."""

    def test_returns_two_dataframes(self, basic_labeled_data):
        """(long_df, short_df) 2-튜플을 반환하는지 확인."""
        result = construct_long_short_df(basic_labeled_data)
        assert len(result) == 2
        long_df, short_df = result
        assert isinstance(long_df, pd.DataFrame)
        assert isinstance(short_df, pd.DataFrame)

    def test_long_df_contains_only_label_1(self, basic_labeled_data):
        """long_df에는 label=1인 종목만 포함되는지 확인."""
        long_df, _ = construct_long_short_df(basic_labeled_data)
        assert (long_df["label"] == 1).all()

    def test_short_df_contains_only_label_neg1(self, basic_labeled_data):
        """short_df에는 label=-1인 종목만 포함되는지 확인."""
        _, short_df = construct_long_short_df(basic_labeled_data)
        assert (short_df["label"] == -1).all()

    def test_neutral_excluded(self, basic_labeled_data):
        """label=0(중립)인 종목은 롱/숏 모두에서 제외되는지 확인."""
        long_df, short_df = construct_long_short_df(basic_labeled_data)
        all_tickers = pd.concat([long_df["ticker"], short_df["ticker"]])
        # C는 label=0이므로 제외
        assert "T_C" not in all_tickers.values


class TestConstructLongShortDfSignal:
    """signal 컬럼 테스트."""

    def test_long_signal_is_l(self, basic_labeled_data):
        """long_df의 signal 컬럼이 'L'인지 확인."""
        long_df, _ = construct_long_short_df(basic_labeled_data)
        assert (long_df["signal"] == "L").all()

    def test_short_signal_is_s(self, basic_labeled_data):
        """short_df의 signal 컬럼이 'S'인지 확인."""
        _, short_df = construct_long_short_df(basic_labeled_data)
        assert (short_df["signal"] == "S").all()


class TestConstructLongShortDfWeights:
    """가중치 계산 테스트."""

    def test_return_weight_equals_label_over_count(self, basic_labeled_data):
        """return_weight = label / 같은 날짜·시그널 내 종목 수."""
        long_df, _ = construct_long_short_df(basic_labeled_data)
        # 2024-01-31: A(L), D(L) → 2개, return_weight = 1/2 = 0.5
        jan_long = long_df[long_df["ddt"] == pd.Timestamp("2024-01-31")]
        assert len(jan_long) == 2
        np.testing.assert_almost_equal(jan_long["return_weight"].iloc[0], 0.5)

    def test_turnover_weight_is_abs_return_weight(self, basic_labeled_data):
        """turnover_weight = abs(return_weight)."""
        long_df, short_df = construct_long_short_df(basic_labeled_data)
        for df in [long_df, short_df]:
            if not df.empty:
                np.testing.assert_array_almost_equal(
                    df["turnover_weight"].values,
                    np.abs(df["return_weight"].values),
                )

    def test_short_return_weight_is_negative(self, basic_labeled_data):
        """short_df의 return_weight는 음수 (label=-1이므로 -1/count)."""
        _, short_df = construct_long_short_df(basic_labeled_data)
        assert (short_df["return_weight"] < 0).all()


class TestConstructLongShortDfDateFilter:
    """날짜 필터링 테스트."""

    def test_dates_before_cutoff_excluded(self, labeled_data_with_early_dates):
        """2017-12-31 이전 데이터가 제외되는지 확인."""
        long_df, short_df = construct_long_short_df(labeled_data_with_early_dates)
        all_dates = pd.concat([long_df["ddt"], short_df["ddt"]])
        assert (all_dates >= pd.Timestamp("2017-12-31")).all()

    def test_cutoff_boundary_included(self, labeled_data_with_early_dates):
        """2017-12-31 경계값은 포함되는지 확인."""
        long_df, short_df = construct_long_short_df(labeled_data_with_early_dates)
        all_dates = pd.concat([long_df["ddt"], short_df["ddt"]])
        assert pd.Timestamp("2017-12-31") in all_dates.values


class TestConstructLongShortDfRequiredColumns:
    """필수 컬럼 존재 테스트."""

    def test_output_columns(self, basic_labeled_data):
        """출력에 필수 컬럼이 존재하는지 확인."""
        long_df, short_df = construct_long_short_df(basic_labeled_data)
        expected_cols = {"signal", "num", "return_weight", "turnover_weight"}
        for df in [long_df, short_df]:
            if not df.empty:
                assert expected_cols.issubset(set(df.columns))


# ═══════════════════════════════════════════════════════════════════════════════
# calculate_vectorized_return Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestCalculateVectorizedReturnBasic:
    """기본 수익률 계산 테스트."""

    def test_returns_three_dataframes(self, multi_date_portfolio):
        """(gross, net, cost) 3-튜플을 반환하는지 확인."""
        result = calculate_vectorized_return(multi_date_portfolio, "TestFactor")
        assert len(result) == 3
        gross, net, cost = result
        assert isinstance(gross, pd.DataFrame)
        assert isinstance(net, pd.DataFrame)
        assert isinstance(cost, pd.DataFrame)

    def test_column_name_matches_factor_abbr(self, multi_date_portfolio):
        """컬럼명이 factor_abbr와 일치하는지 확인."""
        gross, net, cost = calculate_vectorized_return(multi_date_portfolio, "SalesAcc")
        assert "SalesAcc" in gross.columns
        assert "SalesAcc" in net.columns
        assert "SalesAcc" in cost.columns

    def test_first_row_is_zero(self, multi_date_portfolio):
        """첫 번째 행의 gross return이 0인지 확인."""
        gross, _, _ = calculate_vectorized_return(multi_date_portfolio, "TestFactor")
        assert gross.iloc[0, 0] == 0.0


class TestCalculateVectorizedReturnRelationships:
    """수익률 관계 테스트."""

    def test_net_equals_gross_minus_cost(self, multi_date_portfolio):
        """net = gross - cost 관계가 성립하는지 확인."""
        gross, net, cost = calculate_vectorized_return(multi_date_portfolio, "TestFactor")
        expected_net = gross.values - cost.values
        np.testing.assert_array_almost_equal(net.values, expected_net, decimal=10)

    def test_cost_is_non_negative(self, multi_date_portfolio):
        """거래비용이 음수가 아닌지 확인."""
        _, _, cost = calculate_vectorized_return(multi_date_portfolio, "TestFactor")
        assert (cost.values >= -1e-10).all()

    def test_custom_cost_bps(self, multi_date_portfolio):
        """cost_bps=0이면 gross == net인지 확인."""
        gross, net, cost = calculate_vectorized_return(
            multi_date_portfolio, "TestFactor", cost_bps=0.0
        )
        np.testing.assert_array_almost_equal(gross.values, net.values, decimal=10)


class TestCalculateVectorizedReturnShape:
    """출력 형태 테스트."""

    def test_output_is_single_column(self, multi_date_portfolio):
        """각 출력이 단일 컬럼 DataFrame인지 확인."""
        gross, net, cost = calculate_vectorized_return(multi_date_portfolio, "TestFactor")
        assert gross.shape[1] == 1
        assert net.shape[1] == 1
        assert cost.shape[1] == 1

    def test_output_rows_match_dates(self, multi_date_portfolio):
        """출력 행 수가 고유 날짜 수와 일치하는지 확인."""
        gross, _, _ = calculate_vectorized_return(multi_date_portfolio, "TestFactor")
        n_dates = multi_date_portfolio["ddt"].nunique()
        assert gross.shape[0] == n_dates
