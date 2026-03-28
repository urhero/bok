# -*- coding: utf-8 -*-
"""filter_and_label_factors 함수 유닛 테스트.

음의 스프레드 섹터 제거, L/N/S 라벨링, 엣지 케이스를 검증한다.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from service.pipeline.factor_analysis import filter_and_label_factors


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

def _make_sector_return_df(sector_spreads: dict[str, tuple]) -> pd.DataFrame:
    """섹터별 Q1~Q5 수익률로 sector_return_df를 생성한다.

    Args:
        sector_spreads: {섹터이름: (Q1, Q2, Q3, Q4, Q5)} 형태
    """
    data = {}
    for sec, vals in sector_spreads.items():
        data[sec] = vals
    # sector_return_df: index=Q1~Q5, columns=섹터 (calculate_factor_stats 출력 형태)
    # columns.name = "sec" 설정 (groupby("sec") 결과의 .T에서 index.name이 "sec"이 됨)
    df = pd.DataFrame(data, index=["Q1", "Q2", "Q3", "Q4", "Q5"])
    df.columns.name = "sec"
    return df


def _make_raw_df(sectors: list[str], dates: list, quantiles=None) -> pd.DataFrame:
    """테스트용 종목 데이터를 생성한다."""
    np.random.seed(42)
    if quantiles is None:
        quantiles = ["Q1", "Q2", "Q3", "Q4", "Q5"]
    rows = []
    for date in dates:
        for sec in sectors:
            for q in quantiles:
                for j in range(5):
                    # Q1에 높은 수익률, Q5에 낮은 수익률을 부여하여 양의 스프레드 보장
                    base_return = {"Q1": 0.05, "Q2": 0.03, "Q3": 0.01, "Q4": -0.01, "Q5": -0.03}
                    rows.append({
                        "gvkeyiid": f"GV{sec[:2]}{q}{j}",
                        "ticker": f"{sec[:2]}_{q}_{j}",
                        "isin": f"KR{sec[:2]}{q}{j:04d}",
                        "ddt": date,
                        "sec": sec,
                        "country": "KR",
                        "quantile": q,
                        "M_RETURN": base_return.get(q, 0.0) + np.random.randn() * 0.005,
                    })
    return pd.DataFrame(rows)


def _make_raw_df_negative_spread(sectors: list[str], dates: list) -> pd.DataFrame:
    """모든 섹터에서 음의 스프레드를 가지는 종목 데이터를 생성한다."""
    np.random.seed(42)
    quantiles = ["Q1", "Q2", "Q3", "Q4", "Q5"]
    rows = []
    for date in dates:
        for sec in sectors:
            for q in quantiles:
                for j in range(5):
                    # Q1에 낮은 수익률, Q5에 높은 수익률 → 음의 스프레드
                    base_return = {"Q1": -0.03, "Q2": -0.01, "Q3": 0.01, "Q4": 0.03, "Q5": 0.05}
                    rows.append({
                        "gvkeyiid": f"GV{sec[:2]}{q}{j}",
                        "ticker": f"{sec[:2]}_{q}_{j}",
                        "isin": f"KR{sec[:2]}{q}{j:04d}",
                        "ddt": date,
                        "sec": sec,
                        "country": "KR",
                        "quantile": q,
                        "M_RETURN": base_return.get(q, 0.0) + np.random.randn() * 0.002,
                    })
    return pd.DataFrame(rows)


@pytest.fixture
def dates_3m():
    return pd.date_range("2024-01-31", periods=3, freq="ME")


@pytest.fixture
def basic_factor_data(dates_3m):
    """기본 팩터 데이터: 2개 섹터, 양의 스프레드."""
    sectors = ["IT", "Health"]
    sector_ret = _make_sector_return_df({
        "IT": (0.05, 0.03, 0.01, -0.01, -0.03),
        "Health": (0.06, 0.04, 0.02, 0.00, -0.02),
    })
    raw = _make_raw_df(sectors, dates_3m)
    return sector_ret, None, None, raw


@pytest.fixture
def mixed_spread_factor_data(dates_3m):
    """양/음 혼합 스프레드 데이터: IT(양), Finance(음)."""
    sectors = ["IT", "Finance"]
    sector_ret = _make_sector_return_df({
        "IT": (0.05, 0.03, 0.01, -0.01, -0.03),
        "Finance": (-0.01, 0.00, 0.01, 0.02, 0.03),  # Q1 < Q5 → 음의 스프레드
    })
    raw = _make_raw_df(sectors, dates_3m)
    return sector_ret, None, None, raw


@pytest.fixture
def all_negative_factor_data(dates_3m):
    """모든 섹터가 음의 스프레드."""
    sectors = ["SectorA", "SectorB"]
    sector_ret = _make_sector_return_df({
        "SectorA": (-0.02, -0.01, 0.01, 0.02, 0.04),
        "SectorB": (-0.03, -0.01, 0.00, 0.02, 0.05),
    })
    raw = _make_raw_df_negative_spread(sectors, dates_3m)
    return sector_ret, None, None, raw


# ═══════════════════════════════════════════════════════════════════════════════
# Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestFilterAndLabelFactorsBasic:
    """기본 기능 테스트."""

    def test_return_types_and_length(self, basic_factor_data):
        """반환 타입이 올바르고 6-튜플인지 확인."""
        result = filter_and_label_factors(
            ["FactorA"], ["Factor A Name"], ["Valuation"],
            [basic_factor_data],
        )
        assert len(result) == 6
        kept_abbrs, kept_names, kept_styles, kept_idx, dropped_sec, filtered_data = result
        assert isinstance(kept_abbrs, list)
        assert isinstance(kept_names, list)
        assert isinstance(kept_styles, list)
        assert isinstance(kept_idx, list)
        assert isinstance(dropped_sec, list)
        assert isinstance(filtered_data, list)

    def test_single_factor_preserved(self, basic_factor_data):
        """양의 스프레드만 있는 단일 팩터가 유지되는지 확인."""
        kept_abbrs, kept_names, kept_styles, kept_idx, _, filtered_data = filter_and_label_factors(
            ["FactorA"], ["Factor A Name"], ["Valuation"],
            [basic_factor_data],
        )
        assert len(kept_abbrs) == 1
        assert kept_abbrs[0] == "FactorA"
        assert kept_names[0] == "Factor A Name"
        assert kept_styles[0] == "Valuation"
        assert kept_idx == [0]

    def test_filtered_data_has_label_column(self, basic_factor_data):
        """필터링된 데이터에 label 컬럼이 존재하는지 확인."""
        _, _, _, _, _, filtered_data = filter_and_label_factors(
            ["FactorA"], ["Factor A Name"], ["Valuation"],
            [basic_factor_data],
        )
        assert len(filtered_data) == 1
        df = filtered_data[0]
        assert "label" in df.columns

    def test_label_values_are_valid(self, basic_factor_data):
        """label 값이 1, 0, -1 중 하나인지 확인."""
        _, _, _, _, _, filtered_data = filter_and_label_factors(
            ["FactorA"], ["Factor A Name"], ["Valuation"],
            [basic_factor_data],
        )
        df = filtered_data[0]
        valid_labels = {1, 0, -1}
        assert set(df["label"].unique()).issubset(valid_labels)


class TestFilterAndLabelFactorsSectorRemoval:
    """음의 스프레드 섹터 제거 테스트."""

    def test_negative_spread_sector_removed(self, mixed_spread_factor_data):
        """음의 스프레드를 가진 Finance 섹터가 제거되는지 확인."""
        _, _, _, _, dropped_sec, filtered_data = filter_and_label_factors(
            ["FactorA"], ["Factor A Name"], ["Valuation"],
            [mixed_spread_factor_data],
        )
        assert len(dropped_sec) == 1
        assert "Finance" in dropped_sec[0]

    def test_positive_spread_sector_retained(self, mixed_spread_factor_data):
        """양의 스프레드를 가진 IT 섹터가 유지되는지 확인."""
        _, _, _, _, _, filtered_data = filter_and_label_factors(
            ["FactorA"], ["Factor A Name"], ["Valuation"],
            [mixed_spread_factor_data],
        )
        df = filtered_data[0]
        assert "IT" in df["sec"].unique()
        assert "Finance" not in df["sec"].unique()

    def test_all_sectors_dropped_discards_factor(self, all_negative_factor_data):
        """모든 섹터가 음의 스프레드면 팩터 자체가 제거되는지 확인."""
        kept_abbrs, _, _, kept_idx, _, filtered_data = filter_and_label_factors(
            ["BadFactor"], ["Bad Factor Name"], ["Momentum"],
            [all_negative_factor_data],
        )
        assert len(kept_abbrs) == 0
        assert len(kept_idx) == 0
        assert len(filtered_data) == 0


class TestFilterAndLabelFactorsLabeling:
    """L/N/S 라벨 분포 테스트."""

    def test_q1_gets_long_label(self, basic_factor_data):
        """Q1 종목들은 Long(1) 라벨을 받는지 확인."""
        _, _, _, _, _, filtered_data = filter_and_label_factors(
            ["FactorA"], ["Factor A Name"], ["Valuation"],
            [basic_factor_data],
        )
        df = filtered_data[0]
        q1_labels = df[df["quantile"] == "Q1"]["label"].unique()
        assert 1 in q1_labels

    def test_q5_gets_short_label(self, basic_factor_data):
        """Q5 종목들은 Short(-1) 라벨을 받는지 확인."""
        _, _, _, _, _, filtered_data = filter_and_label_factors(
            ["FactorA"], ["Factor A Name"], ["Valuation"],
            [basic_factor_data],
        )
        df = filtered_data[0]
        q5_labels = df[df["quantile"] == "Q5"]["label"].unique()
        assert -1 in q5_labels

    def test_at_least_one_long_and_short(self, basic_factor_data):
        """Long과 Short 라벨이 최소 하나씩 존재하는지 확인."""
        _, _, _, _, _, filtered_data = filter_and_label_factors(
            ["FactorA"], ["Factor A Name"], ["Valuation"],
            [basic_factor_data],
        )
        df = filtered_data[0]
        labels = df["label"].unique()
        assert 1 in labels, "Long label (1) missing"
        assert -1 in labels, "Short label (-1) missing"


class TestFilterAndLabelFactorsMultipleFactors:
    """다중 팩터 입력 테스트."""

    def test_kept_idx_tracks_original_positions(self, basic_factor_data, all_negative_factor_data):
        """kept_idx가 원본 인덱스를 올바르게 추적하는지 확인."""
        factor_data_list = [all_negative_factor_data, basic_factor_data]
        kept_abbrs, _, _, kept_idx, _, _ = filter_and_label_factors(
            ["BadFactor", "GoodFactor"],
            ["Bad Factor", "Good Factor"],
            ["Momentum", "Valuation"],
            factor_data_list,
        )
        # BadFactor(idx=0)는 제거, GoodFactor(idx=1)만 유지
        assert kept_abbrs == ["GoodFactor"]
        assert kept_idx == [1]

    def test_multiple_good_factors(self, basic_factor_data, dates_3m):
        """여러 양의 팩터가 모두 유지되는지 확인."""
        sectors = ["Consumer", "Energy"]
        sector_ret2 = _make_sector_return_df({
            "Consumer": (0.04, 0.02, 0.01, -0.01, -0.02),
            "Energy": (0.03, 0.02, 0.00, -0.01, -0.03),
        })
        raw2 = _make_raw_df(sectors, dates_3m)
        factor_data_2 = (sector_ret2, None, None, raw2)

        kept_abbrs, _, _, kept_idx, _, _ = filter_and_label_factors(
            ["FactorA", "FactorB"],
            ["Factor A", "Factor B"],
            ["Valuation", "Quality"],
            [basic_factor_data, factor_data_2],
        )
        assert len(kept_abbrs) == 2
        assert kept_idx == [0, 1]


class TestFilterAndLabelFactorsEdgeCases:
    """엣지 케이스 테스트."""

    def test_none_data_skipped(self):
        """None 데이터는 스킵되는지 확인."""
        none_data = (None, None, None, None)
        kept_abbrs, _, _, kept_idx, _, _ = filter_and_label_factors(
            ["NullFactor"], ["Null"], ["Style"],
            [none_data],
        )
        assert len(kept_abbrs) == 0
        assert len(kept_idx) == 0

    def test_none_sector_return_skipped(self, dates_3m):
        """sector_return_df가 None이면 스킵."""
        raw = _make_raw_df(["IT"], dates_3m)
        data = (None, None, None, raw)
        kept_abbrs, _, _, _, _, _ = filter_and_label_factors(
            ["PartialFactor"], ["Partial"], ["Style"],
            [data],
        )
        assert len(kept_abbrs) == 0

    def test_none_raw_df_skipped(self, dates_3m):
        """raw_df가 None이면 스킵."""
        sector_ret = _make_sector_return_df({"IT": (0.05, 0.03, 0.01, -0.01, -0.03)})
        data = (sector_ret, None, None, None)
        kept_abbrs, _, _, _, _, _ = filter_and_label_factors(
            ["PartialFactor"], ["Partial"], ["Style"],
            [data],
        )
        assert len(kept_abbrs) == 0

    def test_empty_factor_list(self):
        """빈 입력 리스트 처리."""
        result = filter_and_label_factors([], [], [], [])
        kept_abbrs, kept_names, kept_styles, kept_idx, dropped_sec, filtered_data = result
        assert kept_abbrs == []
        assert kept_idx == []
        assert filtered_data == []
