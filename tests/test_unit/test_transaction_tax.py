# -*- coding: utf-8 -*-
"""국가별 증권거래세 헬퍼 검증 (2026-08-12)."""
import pandas as pd
import pytest

from service.pipeline import transaction_tax as tt


@pytest.fixture
def patched_rates(monkeypatch):
    """UK(매수 50bp) / HK(양방 10bp) / JP(면세) 3종목 세율표를 주입한다."""
    rf = pd.DataFrame(
        {"buy": [50 / 1e4, 10 / 1e4, 0.0], "sell": [0.0, 10 / 1e4, 0.0]},
        index=["UK1", "HK1", "JP1"],
    )
    monkeypatch.setattr(tt, "_rate_frame", lambda benchmark: rf)
    return rf


def test_uk_buy_charged_sell_free(patched_rates):
    """영국은 매수 편측 과세 — 같은 크기의 매도는 0."""
    assert tt.tax_cost(pd.Series({"UK1": 1.0})) == pytest.approx(0.005)
    assert tt.tax_cost(pd.Series({"UK1": -1.0})) == pytest.approx(0.0)


def test_hk_both_sides(patched_rates):
    """홍콩은 양방 과세 — 매수/매도 동일."""
    assert tt.tax_cost(pd.Series({"HK1": 1.0})) == pytest.approx(0.001)
    assert tt.tax_cost(pd.Series({"HK1": -1.0})) == pytest.approx(0.001)


def test_exempt_and_unmapped_are_zero(patched_rates):
    """면세국과 미매핑 종목은 0 (세율표 미등재 = 면세 취급)."""
    assert tt.tax_cost(pd.Series({"JP1": 1.0, "UNKNOWN": -5.0})) == pytest.approx(0.0)


def test_mixed_book_sums_by_direction(patched_rates):
    """혼합 북: 방향별로 각각 집계된다."""
    delta = pd.Series({"UK1": 0.5, "HK1": -0.2, "JP1": 3.0})
    assert tt.tax_cost(delta) == pytest.approx(0.5 * 0.005 + 0.2 * 0.001)


def test_empty_delta(patched_rates):
    assert tt.tax_cost(pd.Series(dtype=float)) == 0.0


def test_excluded_countries_threshold():
    """평균 (매수+매도)/2 기준 임계 이상 국가만 배제. 경계값은 포함(>=)."""
    from service.pipeline.transaction_tax import excluded_countries

    assert excluded_countries(None) == set()
    assert excluded_countries(0) == set()
    # 영(25)·아일랜드(50)·프랑스(20)·남아공(12.5) 는 10 초과, 스페인/이탈리아/홍콩은 정확히 10
    assert excluded_countries(10.0) == {"GBR", "IRL", "FRA", "ZAF", "ESP", "ITA", "HKG"}
    assert excluded_countries(10.01) == {"GBR", "IRL", "FRA", "ZAF"}
    assert excluded_countries(30.0) == {"IRL"}


def test_drop_high_tax_countries_noop_when_off():
    """임계 None 이면 원본을 그대로 반환 (파일 IO 없이 조기 반환)."""
    from service.pipeline.transaction_tax import drop_high_tax_countries

    raw = pd.DataFrame({"gvkeyiid": ["A", "B"], "val": [1.0, 2.0]})
    out = drop_high_tax_countries(raw, "NONEXISTENT_BENCHMARK", None)
    pd.testing.assert_frame_equal(out, raw)


def test_missing_country_map_degrades_to_zero(monkeypatch, tmp_path):
    """국가맵 파일이 없으면 크래시 대신 세금 0 (CI/미매핑 유니버스 대비)."""
    monkeypatch.setattr(tt, "DATA_DIR", tmp_path)
    tt._rate_frame.cache_clear()
    try:
        assert tt.tax_cost(pd.Series({"X": 1.0}), benchmark="NO_SUCH_BM") == 0.0
    finally:
        tt._rate_frame.cache_clear()


def test_drop_high_tax_countries_degrades_without_country_map():
    """임계를 켜도 국가맵 parquet 이 없으면 크래시 대신 원본 반환 (2026-08-25 가드).

    _rate_frame 은 7a0ed44 에서 같은 가드를 받았으나 이 형제 경로는 누락돼 있었음.
    """
    import pandas as pd
    from service.pipeline.transaction_tax import drop_high_tax_countries

    raw = pd.DataFrame({"gvkeyiid": ["a", "b"], "val": [1.0, 2.0]})
    out = drop_high_tax_countries(raw, "NONEXISTENT_BENCHMARK", 10.0)  # 임계 on
    assert out is raw, "국가맵 없음 -> 배제 미적용 (원본 그대로)"


def test_attach_region_dedups_country_map(tmp_path):
    """country map 에 gvkeyiid 중복이 있어도 left merge 가 행을 부풀리지 않아야 한다."""
    import pandas as pd
    from service.pipeline.factor_analysis import attach_region

    cmap_path = tmp_path / "cmap.parquet"
    pd.DataFrame({
        "gvkeyiid": ["g1", "g1", "g2"],   # g1 중복
        "country": ["USA", "USA", "JPN"],
    }).to_parquet(cmap_path)

    merged = pd.DataFrame({"gvkeyiid": ["g1", "g2"], "val": [1.0, 2.0]})
    out = attach_region(merged, cmap_path)
    assert len(out) == 2, f"중복 조인으로 행 부풀림: {len(out)}"
    assert "region" in out.columns
