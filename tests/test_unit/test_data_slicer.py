# -*- coding: utf-8 -*-
"""데이터 슬라이서 단위 테스트."""
from __future__ import annotations

import pandas as pd
import pytest

from service.backtest.data_slicer import get_oos_dates, slice_data_by_date


@pytest.fixture
def sample_raw_data():
    """테스트용 raw 데이터."""
    dates = pd.date_range("2018-01-31", periods=48, freq="ME")
    rows = []
    for d in dates:
        for i in range(3):
            rows.append({"gvkeyiid": f"GV{i}", "ddt": d, "val": i * 10, "sec": "IT"})
    return pd.DataFrame(rows)


@pytest.fixture
def sample_mreturn():
    """테스트용 M_RETURN 데이터."""
    dates = pd.date_range("2018-01-31", periods=48, freq="ME")
    rows = []
    for d in dates:
        for i in range(3):
            rows.append({"gvkeyiid": f"GV{i}", "ddt": d, "M_RETURN": 0.01})
    return pd.DataFrame(rows)


class TestSliceDataByDate:
    def test_slice_excludes_future_data(self, sample_raw_data, sample_mreturn):
        end_date = "2020-01-31"
        sliced, _ = slice_data_by_date(sample_raw_data, sample_mreturn, end_date)
        assert (sliced["ddt"] <= pd.Timestamp(end_date)).all()

    def test_slice_inclusive(self, sample_raw_data, sample_mreturn):
        """end_date가 inclusive인지 확인 (<= not <)."""
        end_date = "2020-01-31"
        sliced, _ = slice_data_by_date(sample_raw_data, sample_mreturn, end_date)
        assert pd.Timestamp(end_date) in sliced["ddt"].values

    def test_slice_returns_copy(self, sample_raw_data, sample_mreturn):
        """반환값이 원본의 복사본인지 확인."""
        sliced, sliced_mret = slice_data_by_date(sample_raw_data, sample_mreturn, "2020-01-31")
        # 수정해도 원본에 영향 없어야 함
        original_len = len(sample_raw_data)
        sliced.drop(sliced.index[:5], inplace=True)
        assert len(sample_raw_data) == original_len


class TestGetOosDates:
    def test_oos_dates_start_after_min_is(self, sample_raw_data):
        all_dates = sorted(sample_raw_data["ddt"].unique())
        oos = get_oos_dates(all_dates, min_is_months=36)
        assert len(oos) == len(all_dates) - 36
        assert oos[0] == all_dates[36]

    def test_edge_case_insufficient_data(self, sample_raw_data):
        all_dates = sorted(sample_raw_data["ddt"].unique())
        with pytest.raises(ValueError):
            get_oos_dates(all_dates, min_is_months=len(all_dates))

    def test_exact_boundary(self, sample_raw_data):
        all_dates = sorted(sample_raw_data["ddt"].unique())
        with pytest.raises(ValueError):
            get_oos_dates(all_dates, min_is_months=len(all_dates))
