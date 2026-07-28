# -*- coding: utf-8 -*-
"""
Unit tests for calculate_factor_stats_batch() function (production 경로).

calculate_factor_stats_batch() 함수 테스트:
- 팩터의 5분위 포트폴리오 구성
- 섹터별/전체 분위수익률 계산
- Q1-Q5 스프레드 계산
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from service.pipeline.factor_analysis import (
    calculate_factor_stats_batch,
)


# ═══════════════════════════════════════════════════════════════════════════
# calculate_factor_stats_batch (production 경로) 직접 단위 테스트
# ═══════════════════════════════════════════════════════════════════════════

def _make_multi_factor_frame(
    n_months: int = 6, n_stocks: int = 8, factors: tuple = ("FA", "FB"), seed: int = 7,
) -> pd.DataFrame:
    """factorAbbreviation 컬럼을 가진 batch 입력 프레임 생성 (2 섹터)."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2024-01-31", periods=n_months, freq="ME")
    rows = []
    for fa in factors:
        for m, d in enumerate(dates):
            for s in range(n_stocks):
                rows.append({
                    "gvkeyiid": f"G{s:02d}",
                    "ddt": d,
                    "sec": "S1" if s < n_stocks // 2 else "S2",
                    "val": float(rng.normal(s, 0.1)),       # 종목별 수준 + 노이즈
                    "M_RETURN": float(rng.normal(0.01 * (s % 3), 0.02)),
                    "factorAbbreviation": fa,
                })
    return pd.DataFrame(rows)


class TestCalculateFactorStatsBatch:
    """batch 버전 직접 테스트: single 버전과의 parity 가 핵심 불변식."""

    def test_insufficient_history_returns_none(self) -> None:
        """월 수 <= 2 (lag 후) 팩터는 (None,)*4."""
        df = _make_multi_factor_frame(n_months=3)   # lag 후 2개월 -> 부족
        batch = calculate_factor_stats_batch(df, ["FA", "FB"], [1, 1], test_mode=True)
        assert batch[0] == (None, None, None, None)
        assert batch[1] == (None, None, None, None)

    def test_missing_factor_returns_none(self) -> None:
        """abbr_list 에 있으나 데이터에 없는 팩터는 (None,)*4, 위치 유지."""
        df = _make_multi_factor_frame(factors=("FA",))
        batch = calculate_factor_stats_batch(df, ["FA", "GHOST"], [1, 1], test_mode=True)
        assert batch[0][0] is not None
        assert batch[1] == (None, None, None, None)

    def test_small_sectors_skipped_for_quintile_coverage(self) -> None:
        """production 모드에서 전 섹터가 min_sector_stocks 미만이면 분위 배정
        불가 -> 'insufficient quintile coverage' skip 분기."""
        df = _make_multi_factor_frame(factors=("FA",), n_stocks=8)  # 섹터당 4 < 10
        batch = calculate_factor_stats_batch(
            df, ["FA"], [1], test_mode=False, min_sector_stocks=10,
        )
        assert batch[0] == (None, None, None, None)

    @staticmethod
    def _sparse_fb_frame() -> pd.DataFrame:
        """FB 의 val 을 8종목 중 4종목만 남긴 프레임 (FB 커버리지 50%)."""
        df = _make_multi_factor_frame()
        sparse = df["gvkeyiid"].isin({"G04", "G05", "G06", "G07"})
        df.loc[(df["factorAbbreviation"] == "FB") & sparse, "val"] = np.nan
        return df

    def test_low_coverage_factor_excluded(self) -> None:
        """min_coverage_pct: 유니버스 대비 유효 관측 비율(FB 50%)이 임계(60%)
        미만이면 (None,)*4. FA(100%)는 유지."""
        batch = calculate_factor_stats_batch(
            self._sparse_fb_frame(), ["FA", "FB"], [1, 1], test_mode=True, min_coverage_pct=0.6,
        )
        assert batch[0][0] is not None
        assert batch[1] == (None, None, None, None)

    def test_coverage_filter_off_by_default(self) -> None:
        """min_coverage_pct 기본값 0.0 이면 희소 팩터도 유지 (기존 동작 보존)."""
        batch = calculate_factor_stats_batch(
            self._sparse_fb_frame(), ["FA", "FB"], [1, 1], test_mode=True,
        )
        assert batch[1][0] is not None

    def test_region_sector_ranking_groups_within_region(self) -> None:
        """ranking_group='region_sector': 분위가 (ddt, region, sec) 그룹 내에서
        결정된다. 두 지역의 val 수준이 크게 달라도 지역별로 Q1~Q5 가 고르게
        나와야 한다 (글로벌 랭킹이면 저값 지역이 Q1 독식)."""
        df = _make_multi_factor_frame(factors=("FA",), n_stocks=8)
        df["sec"] = "S1"  # 단일 섹터로 단순화
        df["region"] = np.where(df["gvkeyiid"].isin({"G00", "G01", "G02", "G03"}), "R1", "R2")
        df.loc[df["region"] == "R2", "val"] += 1000.0  # R2 를 전부 고값으로

        batch = calculate_factor_stats_batch(
            df, ["FA"], [1], test_mode=True, ranking_group="region_sector",
        )
        fdf = batch[0][3]
        per_region_q = fdf.groupby("region", observed=True)["quantile"].nunique()
        assert (per_region_q >= 2).all(), "각 지역 내에서 분위가 나뉘어야 함"

        # 글로벌(sector) 랭킹이면 R1(저값)이 Q1 쪽을 독식 -> region 별 Q1 존재로 구분 검증
        q1_regions = fdf.loc[fdf["quantile"] == "Q1", "region"].unique()
        assert set(q1_regions) == {"R1", "R2"}, "지역 중립이면 양 지역 모두 Q1 보유"

    def test_region_sector_requires_region_column(self) -> None:
        """region 컬럼 없이 region_sector 요청 시 명시적 에러."""
        df = _make_multi_factor_frame(factors=("FA",))
        with pytest.raises(ValueError, match="region"):
            calculate_factor_stats_batch(
                df, ["FA"], [1], test_mode=True, ranking_group="region_sector",
            )

    def test_unmapped_region_rows_excluded(self) -> None:
        """region NaN(미분류 국가) 종목은 분위 배정에서 제외된다."""
        df = _make_multi_factor_frame(factors=("FA",), n_stocks=8)
        df["sec"] = "S1"
        df["region"] = "R1"
        df.loc[df["gvkeyiid"] == "G00", "region"] = np.nan
        batch = calculate_factor_stats_batch(
            df, ["FA"], [1], test_mode=True, ranking_group="region_sector",
        )
        fdf = batch[0][3]
        assert "G00" not in set(fdf["gvkeyiid"]), "NaN region 종목은 분위 제외"

    def test_result_order_follows_abbr_list(self) -> None:
        """결과 리스트 순서는 factor_abbr_list 순서 (데이터 순서 아님)."""
        df = _make_multi_factor_frame(factors=("FB", "FA"))
        batch = calculate_factor_stats_batch(df, ["FA", "FB"], [1, 1], test_mode=True)
        # spread 컬럼명으로 위치 검증
        assert batch[0][2].columns[0] == "FA"
        assert batch[1][2].columns[0] == "FB"


class TestRollingIsStart:
    """walk_forward_engine.rolling_is_start (롤링 IS 윈도우 경계)."""

    def test_none_window_is_expanding(self) -> None:
        from service.backtest.walk_forward_engine import rolling_is_start
        dates = list(pd.date_range("2020-01-31", periods=12, freq="ME"))
        assert rolling_is_start(dates, 5, None) is None
        assert rolling_is_start(dates, 5, 0) is None

    def test_window_slices_last_n_months(self) -> None:
        from service.backtest.walk_forward_engine import rolling_is_start
        dates = list(pd.date_range("2020-01-31", periods=12, freq="ME"))
        # is_end_idx=9 (2020-10-31), window 6 -> 시작 idx 4 (2020-05-31)
        assert rolling_is_start(dates, 9, 6) == dates[4]

    def test_short_history_falls_back_to_full(self) -> None:
        from service.backtest.walk_forward_engine import rolling_is_start
        dates = list(pd.date_range("2020-01-31", periods=12, freq="ME"))
        assert rolling_is_start(dates, 3, 60) == dates[0]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
