# -*- coding: utf-8 -*-
"""산출물 기준일 파일명 헬퍼 (2026-08-19)."""
import pandas as pd

from service.paths import dated, latest


def test_dated_appends_as_of(tmp_path):
    p = tmp_path / "walk_forward_results.csv"
    assert dated(p, "2026-06-30").name == "walk_forward_results_2026-06-30.csv"
    assert dated(p, pd.Timestamp("2026-06-30")).name == "walk_forward_results_2026-06-30.csv"


def test_dated_none_keeps_original(tmp_path):
    p = tmp_path / "meta_data.csv"
    assert dated(p, None) == p


def test_latest_picks_newest_date(tmp_path):
    for d in ("2026-04-30", "2026-06-30", "2026-05-31"):
        (tmp_path / f"meta_data_{d}.csv").write_text("x", encoding="utf-8")
    assert latest(tmp_path / "meta_data.csv").name == "meta_data_2026-06-30.csv"


def test_latest_ignores_non_date_siblings(tmp_path):
    """meta_data_test_test_data.csv 같은 동일 stem 파생 파일은 후보에서 제외."""
    (tmp_path / "meta_data_2026-06-30.csv").write_text("x", encoding="utf-8")
    (tmp_path / "meta_data_test_test_data.csv").write_text("x", encoding="utf-8")
    assert latest(tmp_path / "meta_data.csv").name == "meta_data_2026-06-30.csv"


def test_latest_falls_back_to_undated(tmp_path):
    """날짜 파일이 없으면 구 무날짜 경로 반환 (기존 산출물 호환)."""
    p = tmp_path / "walk_forward_results.csv"
    assert latest(p) == p


def test_latest_as_of_prefers_that_date(tmp_path):
    """as_of 를 주면 그 기준일 이하 중 최신본 — 과거 시점 재현 시 최신본 혼입 방지."""
    for d in ("2025-06-30", "2026-06-30"):
        (tmp_path / f"walk_forward_results_{d}.csv").write_text("x", encoding="utf-8")
    p = tmp_path / "walk_forward_results.csv"
    assert latest(p, "2025-06-30").name == "walk_forward_results_2025-06-30.csv"
    assert latest(p, "2026-06-30").name == "walk_forward_results_2026-06-30.csv"
    assert latest(p).name == "walk_forward_results_2026-06-30.csv"


def test_latest_as_of_earlier_than_all_falls_back(tmp_path):
    (tmp_path / "meta_data_2026-06-30.csv").write_text("x", encoding="utf-8")
    assert latest(tmp_path / "meta_data.csv", "2020-01-01").name == "meta_data_2026-06-30.csv"
