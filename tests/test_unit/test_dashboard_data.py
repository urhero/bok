# -*- coding: utf-8 -*-
"""service/report/dashboard_data.py 및 조립(dashboard.py) 단위/스모크 테스트."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from service.report import dashboard_data as dd


# ── 픽스처 빌더 ────────────────────────────────────────────────────────────

def _curves(n: int = 12) -> pd.DataFrame:
    """walk_forward_results.csv 스키마의 소형 곡선 DataFrame."""
    rets = [0.02, -0.01, 0.03, 0.01, 0.0, 0.02, -0.02, 0.04, 0.01, -0.01, 0.03, 0.02][:n]
    ret = pd.Series(rets)
    cum = (1 + ret).cumprod()
    half = (1 + ret * 0.5).cumprod()  # 선정 EW(약한 버전)
    dates = pd.date_range("2020-01-31", periods=n, freq="ME")
    df = pd.DataFrame(
        {
            "cew_return": ret.values,
            "ew_return": (ret * 0.5).values,
            "ew_all_return": (ret * 0.5).values,
            "ew_top50_return": (ret * 0.7).values,
            "cew_cumulative": cum.values,
            "ew_cumulative": half.values,
            "ew_all_cumulative": half.values,
            "ew_top50_cumulative": (1 + ret * 0.7).cumprod().values,
        },
        index=dates,
    )
    df.index.name = "date"
    return df


def _weights() -> pd.DataFrame:
    """total_aggregated_weights_*.csv 스키마의 소형 가중치 DataFrame."""
    data = [
        ("F1", "A", 0.1, [("T1", 0.05), ("T2", -0.05)]),
        ("F2", "A", 0.2, [("T1", 0.10), ("T3", -0.10)]),
        ("F3", "B", 0.3, [("T2", 0.15), ("T3", -0.15)]),
        ("F4", "B", 0.0, [("T1", 0.0), ("T4", 0.0)]),  # neutral, 제외 대상
    ]
    rows = []
    for factor, style, fw, tks in data:
        for tk, mp in tks:
            rows.append({
                "ddt": "2099-01-31", "ticker": tk, "isin": "X", "gvkeyiid": "Y",
                "mp_ls_weight": mp, "ls_weight": mp, "factor_weight": fw,
                "factor": factor, "style": style, "name": "n", "count": 10,
                "style_ls_weight": mp,
            })
    return pd.DataFrame(rows)


# ── compute_drawdown ───────────────────────────────────────────────────────

def test_compute_drawdown_zero_at_new_high():
    cum = pd.Series([1.0, 1.1, 1.2])
    dd_series = dd.compute_drawdown(cum)
    assert (dd_series <= 1e-12).all()
    assert dd_series.iloc[-1] == pytest.approx(0.0)


def test_compute_drawdown_recovers():
    cum = pd.Series([1.0, 0.8, 0.9])
    dd_series = dd.compute_drawdown(cum)
    assert dd_series.iloc[1] == pytest.approx(-0.2)
    assert dd_series.iloc[2] == pytest.approx(-0.1)


# ── compute_kpis / build_kpis ──────────────────────────────────────────────

def test_compute_kpis_keys_and_ranges():
    k = dd.compute_kpis(_curves())
    for key in ("cagr", "mdd", "sharpe", "calmar", "excess_cagr", "win_rate", "n_months"):
        assert key in k
    assert k["n_months"] == 12
    assert k["mdd"] <= 0.0
    assert 0.0 <= k["win_rate"] <= 1.0
    # win_rate = (cew_return > ew_return[선정 EW]) 비율. 선정 EW = cew*0.5 이므로
    # 양수 달에만 참 -> 양수 8개월 / 12 = 0.667. (음수 달엔 full 손실이 half 보다 커서 false)
    assert k["win_rate"] == pytest.approx(8 / 12)
    assert k["excess_cagr"] > 0.0  # net 양(+) 스트림에서 full > half


def test_build_kpis_prefers_diagnostics_values():
    """진단 파일 값이 있으면 곡선 계산값을 덮어쓴다 (기존 리포트와 일치)."""
    diag = {
        ("OOS 성과 - Constrained EW", "CAGR"): "1.6624%",
        ("OOS 성과 - Constrained EW", "Sharpe"): "0.8026",
        ("Constrained EW vs EW_Top50 비교", "Win Rate"): "49.3827%",
    }
    k = dd.build_kpis(_curves(), diag)
    assert k["cagr"] == pytest.approx(0.016624)
    assert k["sharpe"] == pytest.approx(0.8026)
    assert k["win_rate"] == pytest.approx(0.493827)


def test_build_kpis_merges_funnel_pattern():
    diag = {("1순위 - Funnel Value-Add", "패턴"): "NORMAL"}
    k = dd.build_kpis(_curves(), diag)
    assert k["funnel_pattern"] == "NORMAL"


def test_build_kpis_funnel_blank_without_diag():
    k = dd.build_kpis(_curves(), None)
    assert k["funnel_pattern"] == ""


# ── 현재 포트 집계 ─────────────────────────────────────────────────────────

def test_aggregate_style_weights_dedups_factor():
    s = dd.aggregate_style_weights(_weights())
    assert s["A"] == pytest.approx(0.3)  # F1 0.1 + F2 0.2
    assert s["B"] == pytest.approx(0.3)  # F3 0.3, F4 0.0 제외
    # 내림차순 정렬
    assert list(s.index)[0] in ("A", "B")


def test_active_factors_excludes_zero_weight():
    assert dd.active_factors(_weights()) == {"F1", "F2", "F3"}


def test_factor_tilt_sorted_and_no_zero():
    tilt = dd.factor_tilt(_weights())
    assert list(tilt["factor"]) == ["F3", "F2", "F1"]
    assert "F4" not in set(tilt["factor"])


def test_top_longs_shorts_signs_and_dedup():
    ls = dd.top_longs_shorts(_weights(), n=2)
    by_ticker = dict(zip(ls["ticker"], zip(ls["weight"], ls["side"])))
    assert "T4" not in by_ticker  # net 0 제외
    assert by_ticker["T3"][0] < 0 and by_ticker["T3"][1] == "short"
    assert by_ticker["T1"][0] > 0 and by_ticker["T1"][1] == "long"
    # 티커 중복 없음
    assert ls["ticker"].is_unique


# ── 파일 탐색 / 파싱 ───────────────────────────────────────────────────────

def test_find_latest_weights_file_picks_max_date_excludes_style(tmp_path):
    (tmp_path / "total_aggregated_weights_2025-01-31_test.csv").write_text("x")
    (tmp_path / "total_aggregated_weights_2025-02-28_test.csv").write_text("x")
    (tmp_path / "total_aggregated_weights_style_2025-03-31_test.csv").write_text("x")
    found = dd.find_latest_weights_file(tmp_path)
    assert found is not None
    assert found.name == "total_aggregated_weights_2025-02-28_test.csv"


def test_find_latest_weights_file_respects_end_date(tmp_path):
    (tmp_path / "total_aggregated_weights_2025-01-31_test.csv").write_text("x")
    (tmp_path / "total_aggregated_weights_2025-02-28_test.csv").write_text("x")
    found = dd.find_latest_weights_file(tmp_path, end_date="2025-01-31")
    assert found.name == "total_aggregated_weights_2025-01-31_test.csv"


def test_find_latest_weights_file_none_when_empty(tmp_path):
    assert dd.find_latest_weights_file(tmp_path) is None


def test_snapshot_date_from_path():
    p = Path("output/total_aggregated_weights_2026-05-31_test.csv")
    assert dd.snapshot_date_from_path(p) == "2026-05-31"


def test_parse_diagnostics_roundtrip(tmp_path):
    path = tmp_path / "overfit_diagnostics.csv"
    df = pd.DataFrame(
        [
            ("1순위 - Funnel Value-Add", "패턴", "NORMAL", "정상"),
            ("OOS 성과 - Constrained EW", "Sharpe", "0.8026", ""),
        ],
        columns=["Category", "Metric", "Value", "Interpretation"],
    )
    df.to_csv(path, index=False, encoding="utf-8-sig")
    parsed = dd.parse_diagnostics(path)
    assert parsed[("1순위 - Funnel Value-Add", "패턴")] == "NORMAL"
    assert parsed[("OOS 성과 - Constrained EW", "Sharpe")] == "0.8026"


def test_parse_diagnostics_missing_file(tmp_path):
    assert dd.parse_diagnostics(tmp_path / "nope.csv") == {}


# ── 섹터 분해 ──────────────────────────────────────────────────────────────

def test_sector_net_weights_groups_and_unknown():
    w = pd.DataFrame({
        "ticker": ["T1", "T2", "T3", "T4"],
        "gvkeyiid": ["G1", "G2", "G3", "G9"],
        "mp_ls_weight": [0.10, -0.20, 0.05, 0.03],
    })
    sector_map = {"G1": "Tech", "G2": "Tech", "G3": "Energy"}  # G9 누락
    s = dd.sector_net_weights(w, sector_map)
    assert s["Tech"] == pytest.approx(-0.10)   # 0.10 - 0.20
    assert s["Energy"] == pytest.approx(0.05)
    assert s["Unknown"] == pytest.approx(0.03)  # G9 -> Unknown


def test_load_sector_map_from_parquet(tmp_path):
    df = pd.DataFrame({
        "ddt": pd.to_datetime(["2099-01-31", "2099-01-31", "2099-02-28"]),
        "gvkeyiid": ["G1", "G2", "G1"],
        "ticker": ["T1", "T2", "T1"],
        "isin": ["i1", "i2", "i1"],
        "sec": ["Tech", "Energy", "Tech"],
        "val": [1.0, 2.0, 3.0],
        "factorAbbreviation": ["x", "x", "x"],
        "factorOrder": [0, 0, 0],
    })
    df.to_parquet(tmp_path / "MXCN1A_factor_2099.parquet", index=False)
    m = dd.load_sector_map(tmp_path, "MXCN1A", "2099-01-31")
    assert m == {"G1": "Tech", "G2": "Energy"}


def test_load_sector_map_missing_returns_empty(tmp_path):
    assert dd.load_sector_map(tmp_path, "MXCN1A", "2099-01-31") == {}


# ── 백테스트 가중치 추이 / 회전율 ──────────────────────────────────────────

def _weight_history() -> pd.DataFrame:
    dates = pd.date_range("2020-01-31", periods=4, freq="ME")
    return pd.DataFrame(
        {"F1": [0.5, 0.5, 0.3, 0.3], "F2": [0.5, 0.5, 0.7, 0.7], "F3": [None, None, None, None]},
        index=pd.Index(dates, name="date"),
    )


def test_compute_turnover():
    t = dd.compute_turnover(_weight_history())
    assert t.iloc[0] == 0.0           # 첫 기간 0
    assert t.iloc[1] == pytest.approx(0.0)
    assert t.iloc[2] == pytest.approx(0.2)  # 0.5*(|0.3-0.5|+|0.7-0.5|)
    assert t.iloc[3] == pytest.approx(0.0)


def test_style_weight_history_buckets():
    sh = dd.style_weight_history(_weight_history(), {"F1": "A", "F2": "B"})
    assert set(sh.columns) == {"A", "B", "Unknown"}
    assert sh["A"].iloc[2] == pytest.approx(0.3)
    assert sh["B"].iloc[2] == pytest.approx(0.7)
    assert sh["Unknown"].sum() == pytest.approx(0.0)  # F3 전부 NaN -> 0


def test_factor_style_map(tmp_path):
    path = tmp_path / "factor_info.csv"
    pd.DataFrame({
        "factorId": [0, 1],
        "factorAbbreviation": ["F1", "F2"],
        "factorName": ["n1", "n2"],
        "factorShortName": ["s1", "s2"],
        "styleName": ["A", "B"],
        "factorOrder": [0, 0],
    }).to_csv(path, index=False)
    m = dd.factor_style_map(path)
    assert m == {"F1": "A", "F2": "B"}


# ── build_dashboard 스모크 ─────────────────────────────────────────────────

def test_build_dashboard_smoke(tmp_path):
    # 백테스트 + 현재 포트 픽스처를 tmp output 디렉토리에 기록
    _curves().reset_index().to_csv(tmp_path / "walk_forward_results.csv", index=False)
    _weight_history().to_csv(tmp_path / "walk_forward_weight_history.csv")
    _weights().to_csv(tmp_path / "total_aggregated_weights_2099-01-31_test.csv", index=True)
    pd.DataFrame(
        {
            "factorAbbreviation": ["F1", "F2", "F9"],
            "factorName": ["f1", "f2", "f9"],
            "styleName": ["A", "A", "B"],
            "cagr": [0.1, 0.08, 0.02],
            "tstat": [5.0, 4.0, 1.0],
            "newey_west_tstat": [4.5, 3.5, 0.9],
            "rank_score": [5.0, 4.0, 1.0],
            "rank_style": [1.0, 2.0, 1.0],
            "rank_total": [1.0, 2.0, 3.0],
        }
    ).to_csv(tmp_path / "meta_data.csv", index=False)

    from service.report.dashboard import build_dashboard

    out = build_dashboard(output_dir=tmp_path)
    assert out.exists()
    assert out.name == "dashboard_2099-01-31.html"
    html = out.read_text(encoding="utf-8")
    assert "백테스트" in html
    assert "현재 포트" in html
    assert "Plotly" in html  # plotly.js 인라인 임베드 확인
