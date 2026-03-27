# -*- coding: utf-8 -*-
"""parquet_io 모듈 테스트.

연도별 분할 저장/로드 및 데이터 무결성 검증 기능을 테스트한다.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from service.download.parquet_io import (
    list_yearly_parquets,
    load_factor_parquet,
    save_factor_parquet_by_year,
    validate_loaded_factor_data,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def sample_factor_df():
    """3개 연도에 걸친 sample factor DataFrame (연속 월, gap 없음)."""
    dates = pd.to_datetime([
        "2023-10-31", "2023-11-30", "2023-12-31",
        "2024-01-31", "2024-02-29", "2024-03-31",
        "2024-04-30", "2024-05-31", "2024-06-30",
        "2024-07-31", "2024-08-31", "2024-09-30",
        "2024-10-31", "2024-11-30", "2024-12-31",
        "2025-01-31", "2025-02-28",
    ])
    n_stocks = 5
    n_factors = 3
    rows = []
    for dt in dates:
        for stock_i in range(n_stocks):
            for factor_i in range(n_factors):
                rows.append({
                    "gvkeyiid": f"stock_{stock_i}",
                    "ticker": f"TK{stock_i}",
                    "isin": f"ISIN{stock_i}",
                    "ddt": dt,
                    "sec": "Technology",
                    "val": np.random.randn(),
                    "factorAbbreviation": f"FACTOR_{factor_i}",
                    "factorOrder": factor_i + 1,
                })
    return pd.DataFrame(rows)


@pytest.fixture
def clean_factor_df(sample_factor_df):
    """검증 테스트용 — min_factors_per_month=3, min_stocks_per_month=5 충족."""
    return sample_factor_df


# ═══════════════════════════════════════════════════════════════════════════════
# Save / Load 테스트
# ═══════════════════════════════════════════════════════════════════════════════

class TestSaveFactorParquetByYear:
    """save_factor_parquet_by_year 함수 테스트."""

    def test_creates_correct_files(self, sample_factor_df, tmp_path):
        """연도별 파일이 올바르게 생성되는지 확인."""
        saved = save_factor_parquet_by_year(sample_factor_df, tmp_path, "TEST")
        assert len(saved) == 3  # 2023, 2024, 2025
        expected_names = {"TEST_factor_2023.parquet", "TEST_factor_2024.parquet", "TEST_factor_2025.parquet"}
        actual_names = {p.name for p in saved}
        assert actual_names == expected_names

    def test_roundtrip_preserves_data(self, sample_factor_df, tmp_path):
        """저장 후 로드하면 동일한 데이터가 반환되는지 확인."""
        save_factor_parquet_by_year(sample_factor_df, tmp_path, "TEST")
        loaded = load_factor_parquet(tmp_path, "TEST")

        # 행 수 동일
        assert len(loaded) == len(sample_factor_df)

        # 컬럼 동일 (순서 무관)
        assert set(loaded.columns) == set(sample_factor_df.columns)

        # 값 비교 (정렬 후)
        sort_cols = ["ddt", "gvkeyiid", "factorAbbreviation"]
        orig = sample_factor_df.sort_values(sort_cols).reset_index(drop=True)
        result = loaded.sort_values(sort_cols).reset_index(drop=True)
        pd.testing.assert_frame_equal(orig, result, check_dtype=False)

    def test_save_with_years_filter(self, sample_factor_df, tmp_path):
        """특정 연도만 저장."""
        saved = save_factor_parquet_by_year(
            sample_factor_df, tmp_path, "TEST", years={2024}
        )
        assert len(saved) == 1
        assert saved[0].name == "TEST_factor_2024.parquet"

        # 2024 데이터만 있는지 확인
        loaded = pd.read_parquet(saved[0])
        years_in_data = pd.to_datetime(loaded["ddt"]).dt.year.unique()
        assert list(years_in_data) == [2024]

    def test_row_counts_per_year(self, sample_factor_df, tmp_path):
        """각 연도 파일의 행 수가 올바른지 확인."""
        save_factor_parquet_by_year(sample_factor_df, tmp_path, "TEST")

        # 2023: 3개월 × 5종목 × 3팩터 = 45
        df_2023 = pd.read_parquet(tmp_path / "TEST_factor_2023.parquet")
        assert len(df_2023) == 45

        # 2024: 12개월 × 5종목 × 3팩터 = 180
        df_2024 = pd.read_parquet(tmp_path / "TEST_factor_2024.parquet")
        assert len(df_2024) == 180

        # 2025: 2개월 × 5종목 × 3팩터 = 30
        df_2025 = pd.read_parquet(tmp_path / "TEST_factor_2025.parquet")
        assert len(df_2025) == 30

    def test_no_year_column_in_output(self, sample_factor_df, tmp_path):
        """출력 parquet에 임시 _year 컬럼이 포함되지 않는지 확인."""
        save_factor_parquet_by_year(sample_factor_df, tmp_path, "TEST")
        loaded = pd.read_parquet(tmp_path / "TEST_factor_2023.parquet")
        assert "_year" not in loaded.columns


class TestLoadFactorParquet:
    """load_factor_parquet 함수 테스트."""

    def test_load_split_files(self, sample_factor_df, tmp_path):
        """분할 파일 로드."""
        save_factor_parquet_by_year(sample_factor_df, tmp_path, "TEST")
        result = load_factor_parquet(tmp_path, "TEST")
        assert len(result) == len(sample_factor_df)

    def test_load_with_year_range(self, sample_factor_df, tmp_path):
        """start_year/end_year 필터링."""
        save_factor_parquet_by_year(sample_factor_df, tmp_path, "TEST")
        result = load_factor_parquet(tmp_path, "TEST", start_year=2024, end_year=2024)
        years = pd.to_datetime(result["ddt"]).dt.year.unique()
        assert list(years) == [2024]

    def test_load_fallback_single_file(self, sample_factor_df, tmp_path):
        """단일 파일 fallback."""
        single_path = tmp_path / "TEST_factor.parquet"
        sample_factor_df.to_parquet(single_path, index=False)

        result = load_factor_parquet(tmp_path, "TEST")
        assert len(result) == len(sample_factor_df)

    def test_load_split_priority_over_single(self, sample_factor_df, tmp_path):
        """분할 파일이 단일 파일보다 우선."""
        # 단일 파일 (빈 데이터)
        single_path = tmp_path / "TEST_factor.parquet"
        pd.DataFrame(columns=sample_factor_df.columns).to_parquet(single_path, index=False)

        # 분할 파일 (실제 데이터)
        save_factor_parquet_by_year(sample_factor_df, tmp_path, "TEST")

        result = load_factor_parquet(tmp_path, "TEST")
        assert len(result) == len(sample_factor_df)  # 분할 파일에서 로드

    def test_load_not_found(self, tmp_path):
        """파일 없으면 FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_factor_parquet(tmp_path, "NONEXISTENT")

    def test_load_empty_year_range(self, sample_factor_df, tmp_path):
        """범위에 해당하는 파일이 없으면 FileNotFoundError."""
        save_factor_parquet_by_year(sample_factor_df, tmp_path, "TEST")
        with pytest.raises(FileNotFoundError):
            load_factor_parquet(tmp_path, "TEST", start_year=2030, end_year=2030)

    def test_load_with_validate(self, sample_factor_df, tmp_path):
        """validate=True로 로드 시 정상 데이터는 통과."""
        save_factor_parquet_by_year(sample_factor_df, tmp_path, "TEST")
        # min_factors_per_month=3, min_stocks_per_month=5 충족하므로 통과해야 함
        result = load_factor_parquet(tmp_path, "TEST", validate=True)
        assert len(result) == len(sample_factor_df)


class TestListYearlyParquets:
    """list_yearly_parquets 함수 테스트."""

    def test_returns_sorted_list(self, sample_factor_df, tmp_path):
        """정렬된 파일 리스트 반환."""
        save_factor_parquet_by_year(sample_factor_df, tmp_path, "TEST")
        files = list_yearly_parquets(tmp_path, "TEST")
        assert len(files) == 3
        assert files[0].name < files[1].name < files[2].name

    def test_empty_directory(self, tmp_path):
        """빈 디렉토리면 빈 리스트."""
        assert list_yearly_parquets(tmp_path, "TEST") == []

    def test_ignores_non_matching_files(self, sample_factor_df, tmp_path):
        """이름이 안 맞는 파일은 무시."""
        save_factor_parquet_by_year(sample_factor_df, tmp_path, "TEST")
        # 다른 벤치마크 파일 생성
        (tmp_path / "OTHER_factor_2023.parquet").write_bytes(b"dummy")
        files = list_yearly_parquets(tmp_path, "TEST")
        assert len(files) == 3  # TEST 파일만


# ═══════════════════════════════════════════════════════════════════════════════
# 검증 테스트
# ═══════════════════════════════════════════════════════════════════════════════

class TestValidateLoadedFactorData:
    """validate_loaded_factor_data 함수 테스트."""

    def test_clean_data_no_issues(self, clean_factor_df):
        """정상 데이터 → 빈 issues 리스트."""
        issues = validate_loaded_factor_data(
            clean_factor_df,
            min_factors_per_month=3,
            min_stocks_per_month=5,
        )
        assert len(issues) == 0

    def test_missing_columns(self):
        """필수 컬럼 누락 시 ERROR."""
        df = pd.DataFrame({"ddt": [1], "val": [1.0]})
        issues = validate_loaded_factor_data(df)
        assert any(i["type"] == "MISSING_COLUMNS" and i["level"] == "ERROR" for i in issues)

    def test_insufficient_months(self):
        """월 수 부족 시 ERROR."""
        df = pd.DataFrame({
            "gvkeyiid": ["s1"] * 2,
            "ddt": pd.to_datetime(["2023-01-31", "2023-02-28"]),
            "factorAbbreviation": ["F1"] * 2,
            "val": [1.0, 2.0],
            "sec": ["Tech"] * 2,
        })
        issues = validate_loaded_factor_data(df, min_months=3)
        assert any(i["type"] == "INSUFFICIENT_MONTHS" for i in issues)

    def test_high_null_pct(self):
        """NaN 비율 초과 시 ERROR."""
        n = 100
        df = pd.DataFrame({
            "gvkeyiid": [f"s{i}" for i in range(n)],
            "ddt": pd.to_datetime(["2023-01-31"] * n),
            "factorAbbreviation": [f"F{i % 50}" for i in range(n)],
            "val": [np.nan if i < 10 else 1.0 for i in range(n)],  # 10% NaN
            "sec": ["Tech"] * n,
        })
        issues = validate_loaded_factor_data(df, max_null_pct=0.05, min_months=1)
        assert any(i["type"] == "HIGH_NULL_PCT" for i in issues)

    def test_inf_values(self):
        """inf 값 존재 시 ERROR."""
        df = pd.DataFrame({
            "gvkeyiid": ["s1", "s2", "s3"],
            "ddt": pd.to_datetime(["2023-01-31"] * 3),
            "factorAbbreviation": ["F1"] * 3,
            "val": [1.0, np.inf, 3.0],
            "sec": ["Tech"] * 3,
        })
        issues = validate_loaded_factor_data(df, min_months=1)
        assert any(i["type"] == "INF_VALUES" for i in issues)

    def test_duplicate_rows(self):
        """중복 행 존재 시 ERROR."""
        df = pd.DataFrame({
            "gvkeyiid": ["s1", "s1"],
            "ddt": pd.to_datetime(["2023-01-31"] * 2),
            "factorAbbreviation": ["F1", "F1"],  # 중복
            "val": [1.0, 2.0],
            "sec": ["Tech"] * 2,
        })
        issues = validate_loaded_factor_data(df, min_months=1)
        assert any(i["type"] == "DUPLICATE_ROWS" for i in issues)

    def test_month_gap(self):
        """월 gap 감지 시 ERROR."""
        df = pd.DataFrame({
            "gvkeyiid": ["s1"] * 3,
            "ddt": pd.to_datetime(["2023-01-31", "2023-02-28", "2023-05-31"]),  # 2월→5월 gap
            "factorAbbreviation": ["F1"] * 3,
            "val": [1.0, 2.0, 3.0],
            "sec": ["Tech"] * 3,
        })
        issues = validate_loaded_factor_data(df, min_months=3)
        assert any(i["type"] == "MONTH_GAP" for i in issues)

    def test_low_factor_count_warn(self):
        """팩터 수 부족 시 WARN."""
        df = pd.DataFrame({
            "gvkeyiid": ["s1"] * 3,
            "ddt": pd.to_datetime(["2023-01-31", "2023-02-28", "2023-03-31"]),
            "factorAbbreviation": ["F1"] * 3,
            "val": [1.0, 2.0, 3.0],
            "sec": ["Tech"] * 3,
        })
        issues = validate_loaded_factor_data(df, min_factors_per_month=5, min_stocks_per_month=1)
        assert any(i["type"] == "LOW_FACTOR_COUNT" and i["level"] == "WARN" for i in issues)

    def test_low_stock_count_warn(self):
        """종목 수 부족 시 WARN."""
        df = pd.DataFrame({
            "gvkeyiid": ["s1"] * 3,
            "ddt": pd.to_datetime(["2023-01-31", "2023-02-28", "2023-03-31"]),
            "factorAbbreviation": ["F1"] * 3,
            "val": [1.0, 2.0, 3.0],
            "sec": ["Tech"] * 3,
        })
        issues = validate_loaded_factor_data(df, min_stocks_per_month=5, min_factors_per_month=1)
        assert any(i["type"] == "LOW_STOCK_COUNT" and i["level"] == "WARN" for i in issues)

    def test_load_with_validate_error_raises(self, tmp_path):
        """validate=True + ERROR → RuntimeError."""
        # 필수 컬럼 누락 데이터
        bad_df = pd.DataFrame({"ddt": pd.to_datetime(["2023-01-31"]), "val": [1.0]})
        bad_path = tmp_path / "TEST_factor_2023.parquet"
        bad_df.to_parquet(bad_path, index=False)

        with pytest.raises(RuntimeError, match="error"):
            load_factor_parquet(tmp_path, "TEST", validate=True)
