# -*- coding: utf-8 -*-
"""serialize_diagnostics_csv 회귀 테스트 (restructure Phase 2).

main.py 인라인 직렬화 로직을 overfit_diagnostics.serialize_diagnostics_csv 로
글자보존 추출했다. 이 테스트는 직렬화 산출물이 dashboard_data 의 파싱 계약
(parse_diagnostics -> build_kpis override)을 그대로 만족하는지 고정한다.
한국어 Category/Metric 키가 한 글자라도 바뀌면 viz 대시보드가 조용히 깨지므로
회귀로 차단한다.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from service.backtest.overfit_diagnostics import serialize_diagnostics_csv
from service.report.dashboard_data import build_kpis, parse_diagnostics

# build_kpis 가 override 로 읽는 진단값 (곡선 계산값과 다른 고유값으로 둬서
# override 가 실제로 적용됐는지 확인).
_REPORT = {
    "funnel_pattern": "NORMAL",
    "funnel_interpretation": "정상 패턴",
    "funnel_ew_all_cagr": 0.05,
    "funnel_ew_top50_cagr": 0.07,
    "funnel_cew_cagr": 0.09,
    "funnel_ew_all_mdd": -0.20,
    "funnel_ew_top50_mdd": -0.18,
    "funnel_cew_mdd": -0.15,
    "oos_avg_percentile": 0.55,
    "oos_percentile_interpretation": "양호",
    "strict_jaccard": 0.42,
    "strict_jaccard_interpretation": "안정",
    "is_oos_rank_spearman": 0.33,
    "rank_corr_interpretation": "약한 양",
    "rank_corr_p_value": 0.04,
    "deflation_ratio": 0.88,
    "deflation_interpretation": "보조",
    "oos_cagr": 0.1234,
    "oos_mdd": -0.0567,
    "oos_sharpe": 0.8900,
    "oos_calmar": 1.2345,
    "oos_ew_cagr": 0.1000,
    "oos_ew_mdd": -0.0700,
    "oos_ew_sharpe": 0.7000,
    "cew_vs_ew_excess_cagr": 0.0210,
    "cew_vs_ew_win_rate": 0.6300,
    "warning": "경고 텍스트",
    "limitation": "한계 텍스트",
}


def _write(tmp_path):
    path = tmp_path / "overfit_diagnostics.csv"
    serialize_diagnostics_csv(_REPORT, path)
    return path


def test_csv_has_bom_and_columns(tmp_path):
    """utf-8-sig(BOM) 인코딩 + 세로형 4컬럼 계약 유지."""
    path = _write(tmp_path)
    assert path.read_bytes().startswith(b"\xef\xbb\xbf")  # utf-8-sig BOM
    df = pd.read_csv(path, encoding="utf-8-sig")
    assert list(df.columns) == ["Category", "Metric", "Value", "Interpretation"]
    assert len(df) == 23  # main.py 인라인 rows 개수와 동일


def test_parse_diagnostics_roundtrip(tmp_path):
    """parse_diagnostics 가 build_kpis 가 의존하는 (Category, Metric) 키를 복원."""
    diag = parse_diagnostics(_write(tmp_path))
    assert diag[("1순위 - Funnel Value-Add", "패턴")] == "NORMAL"
    assert diag[("OOS 성과 - Constrained EW", "CAGR")] == "12.3400%"
    assert diag[("OOS 성과 - Constrained EW", "Sharpe")] == "0.8900"
    assert diag[("Constrained EW vs EW_Top50 비교", "Win Rate")] == "63.0000%"


def test_build_kpis_applies_overrides(tmp_path):
    """build_kpis 가 진단 파일 값으로 곡선 계산값을 override (곡선값과 다른지로 확인)."""
    diag = parse_diagnostics(_write(tmp_path))
    # 곡선: 진단값과 명백히 다른 더미 (override 적용을 검증하려는 의도)
    curves = pd.DataFrame(
        {
            "cew_return": [0.01, -0.02, 0.03, 0.01],
            "cew_cumulative": [1.01, 0.99, 1.02, 1.03],
            "ew_return": [0.00, -0.01, 0.02, 0.00],
            "ew_cumulative": [1.00, 0.99, 1.01, 1.01],
        }
    )
    k = build_kpis(curves, diag)
    assert k["cagr"] == 0.1234
    assert k["mdd"] == -0.0567
    assert k["sharpe"] == 0.89
    assert k["calmar"] == 1.2345
    assert k["win_rate"] == 0.63
    assert k["excess_cagr"] == 0.021
    assert k["funnel_pattern"] == "NORMAL"
