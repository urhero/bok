# -*- coding: utf-8 -*-
"""대시보드 데이터 레이어 (read-only).

기존 output/*.csv 만 읽어 정돈된 DataFrame 과 파생 지표(낙폭/KPI/스타일 집계/
상위 롱숏/팩터 틸트/선정 팩터/진단 파싱)를 만든다. plotly 의존성이 없어
단위 테스트가 쉽다. 파이프라인 코드는 일절 건드리지 않는다.
"""
from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd

# service/report/dashboard_data.py -> 프로젝트 루트 / {output, data}
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = _PROJECT_ROOT / "output"
DATA_DIR = _PROJECT_ROOT / "data"

_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


# ── 파일 탐색 ──────────────────────────────────────────────────────────────

def find_latest_weights_file(output_dir: Path, end_date: str | None = None) -> Path | None:
    """total_aggregated_weights_<date>[suffix].csv 중 최신(또는 지정일) 파일 반환.

    _style 변형은 제외. 파일명에서 날짜를 파싱해 최대 날짜를 고르고,
    동일 날짜가 여럿이면 수정시각(mtime)이 최신인 파일을 택한다.
    """
    output_dir = Path(output_dir)
    candidates: list[tuple[str, float, Path]] = []
    for p in output_dir.glob("total_aggregated_weights_*.csv"):
        if p.name.startswith("total_aggregated_weights_style"):
            continue
        m = _DATE_RE.search(p.name)
        if not m:
            continue
        d = m.group(1)
        if end_date is not None and d != end_date:
            continue
        candidates.append((d, p.stat().st_mtime, p))
    if not candidates:
        return None
    candidates.sort(key=lambda t: (t[0], t[1]))
    return candidates[-1][2]


def snapshot_date_from_path(path: Path) -> str | None:
    """파일명에서 YYYY-MM-DD 스냅샷 날짜 추출."""
    m = _DATE_RE.search(Path(path).name)
    return m.group(1) if m else None


# ── 백테스트 ───────────────────────────────────────────────────────────────

def load_backtest_curves(path: Path) -> pd.DataFrame:
    """walk_forward_results.csv -> date 인덱스 DataFrame."""
    df = pd.read_csv(path, parse_dates=["date"])
    return df.set_index("date").sort_index()


def compute_drawdown(cum: pd.Series) -> pd.Series:
    """누적수익 곡선 -> 낙폭 시계열 (running max 대비 비율, <= 0)."""
    cum = pd.Series(cum).astype(float)
    running_max = cum.cummax()
    return cum / running_max - 1.0


def compute_kpis(curves: pd.DataFrame) -> dict:
    """CEW 곡선에서 CAGR/MDD/Sharpe/Calmar/승률/초과CAGR 계산.

    overfit_diagnostics.csv 와 동일 공식 (연 12개월 기준).
    """
    r = curves["cew_return"].astype(float)
    cum = curves["cew_cumulative"].astype(float)
    n = len(r)
    cagr = float(cum.iloc[-1] ** (12.0 / n) - 1.0) if n else float("nan")
    dd = compute_drawdown(cum)
    mdd = float(dd.min())
    std = float(r.std())
    sharpe = float(r.mean() / std * np.sqrt(12)) if std > 0 else float("nan")
    calmar = float(cagr / abs(mdd)) if mdd != 0 else float("nan")

    # 초과수익/승률은 진단(compare_cew_vs_ew_oos)과 동일하게 '선정 EW'(ew_*) 기준.
    ew_cum = curves["ew_cumulative"].astype(float)
    ew_cagr = float(ew_cum.iloc[-1] ** (12.0 / n) - 1.0) if n else float("nan")
    excess_cagr = cagr - ew_cagr
    win_rate = float(
        (curves["cew_return"].astype(float) > curves["ew_return"].astype(float)).mean()
    )
    return {
        "cagr": cagr,
        "mdd": mdd,
        "sharpe": sharpe,
        "calmar": calmar,
        "excess_cagr": excess_cagr,
        "win_rate": win_rate,
        "n_months": n,
    }


def parse_diagnostics(path: Path) -> dict:
    """overfit_diagnostics.csv (세로형) -> {(category, metric): value_str}."""
    p = Path(path)
    if not p.exists():
        return {}
    df = pd.read_csv(p, encoding="utf-8-sig")
    out: dict[tuple[str, str], str] = {}
    for _, row in df.iterrows():
        out[(str(row["Category"]).strip(), str(row["Metric"]).strip())] = str(row["Value"]).strip()
    return out


def _diag_num(diag: dict, category: str, metric: str) -> float | None:
    """진단 Value 문자열('1.66%' / '0.80')을 float 으로. 없거나 N/A 면 None."""
    v = diag.get((category, metric))
    if v is None:
        return None
    v = str(v).strip()
    if v in ("", "N/A", "nan"):
        return None
    try:
        return float(v[:-1]) / 100.0 if v.endswith("%") else float(v)
    except ValueError:
        return None


def build_kpis(curves: pd.DataFrame, diag: dict | None = None) -> dict:
    """곡선에서 KPI 계산 후, 진단파일(overfit_diagnostics.csv) 값이 있으면 그것을 우선.

    진단 파일은 사용자의 기존 리포트 기준값이므로, 일치시켜 혼선을 막는다.
    진단 파일이 없으면(test 모드 등) 곡선 계산값을 그대로 쓴다.
    """
    k = compute_kpis(curves)
    diag = diag or {}
    cew = "OOS 성과 - Constrained EW"
    cmp_ = "Constrained EW vs EW_Top50 비교"
    overrides = {
        "cagr": _diag_num(diag, cew, "CAGR"),
        "mdd": _diag_num(diag, cew, "MDD"),
        "sharpe": _diag_num(diag, cew, "Sharpe"),
        "calmar": _diag_num(diag, cew, "Calmar"),
        "win_rate": _diag_num(diag, cmp_, "Win Rate"),
        "excess_cagr": _diag_num(diag, cmp_, "Excess CAGR"),
    }
    for key, val in overrides.items():
        if val is not None:
            k[key] = val
    k["funnel_pattern"] = diag.get(("1순위 - Funnel Value-Add", "패턴"), "")
    return k


# ── 현재 포트 / 배팅 ───────────────────────────────────────────────────────

def load_weights(path: Path) -> pd.DataFrame:
    """total_aggregated_weights_*.csv -> 무명 인덱스열 제거한 DataFrame."""
    df = pd.read_csv(path)
    drop = [c for c in df.columns if str(c).startswith("Unnamed")]
    if drop:
        df = df.drop(columns=drop)
    return df


def active_factors(weights: pd.DataFrame) -> set:
    """factor_weight > 0 인 (선정된) 팩터 집합."""
    fw = weights[weights["factor_weight"] > 0]
    return set(fw["factor"].unique())


def aggregate_style_weights(weights: pd.DataFrame) -> pd.Series:
    """스타일별 배분: 팩터 단위로 dedup 후 factor_weight 를 style 로 합산 (합 ~= 1)."""
    uniq = weights[["factor", "style", "factor_weight"]].drop_duplicates(subset=["factor"])
    uniq = uniq[uniq["factor_weight"] > 0]
    return uniq.groupby("style")["factor_weight"].sum().sort_values(ascending=False)


def factor_tilt(weights: pd.DataFrame, top_n: int | None = None) -> pd.DataFrame:
    """팩터별 비중(factor_weight) 내림차순 (factor, style, factor_weight)."""
    uniq = weights[["factor", "style", "factor_weight"]].drop_duplicates(subset=["factor"])
    uniq = uniq[uniq["factor_weight"] > 0].sort_values("factor_weight", ascending=False)
    if top_n:
        uniq = uniq.head(top_n)
    return uniq.reset_index(drop=True)


def top_longs_shorts(weights: pd.DataFrame, n: int = 15) -> pd.DataFrame:
    """종목별 순비중(mp_ls_weight 합) 상위 롱 n + 하위(숏) n -> (ticker, weight, side)."""
    net = weights.groupby("ticker")["mp_ls_weight"].sum()
    net = net[net != 0]
    longs = net.sort_values(ascending=False).head(n)
    shorts = net.sort_values().head(n)
    parts = [
        pd.DataFrame({"ticker": longs.index, "weight": longs.values, "side": "long"}),
        pd.DataFrame({"ticker": shorts.index, "weight": shorts.values, "side": "short"}),
    ]
    out = pd.concat(parts, ignore_index=True)
    return out.drop_duplicates(subset=["ticker"]).reset_index(drop=True)


def load_meta(path: Path) -> pd.DataFrame:
    """meta_data.csv -> 팩터 랭킹 테이블."""
    return pd.read_csv(path)


def load_style_deltas(output_dir: Path, snapshot_date: str) -> pd.DataFrame | None:
    """mp_weight_history/style_totals_<date>.csv (전월대비 delta). 없으면 None (test 모드)."""
    p = Path(output_dir) / "mp_weight_history" / f"style_totals_{snapshot_date}.csv"
    if not p.exists():
        return None
    return pd.read_csv(p)


# ── 섹터 분해 (소스 parquet read-only join) ─────────────────────────────────

def load_sector_map(data_dir: Path, benchmark: str, snapshot_date: str) -> dict:
    """소스 factor parquet에서 해당 스냅샷 날짜의 gvkeyiid -> sec 매핑.

    파이프라인을 건드리지 않고 기존 parquet 을 read-only 로 읽어 join 재료를 만든다.
    parquet 이 없거나 날짜/sec 가 없으면 빈 dict (섹터 차트는 생략됨).
    """
    try:
        from service.download.parquet_io import load_factor_parquet
        year = int(snapshot_date[:4])
        df = load_factor_parquet(data_dir, benchmark, start_year=year, end_year=year)
    except (FileNotFoundError, ValueError, ImportError):
        return {}
    if "sec" not in df.columns or "gvkeyiid" not in df.columns:
        return {}
    same_day = pd.to_datetime(df["ddt"]).dt.strftime("%Y-%m-%d") == snapshot_date
    df = df[same_day]
    if df.empty:
        return {}
    uniq = df.drop_duplicates("gvkeyiid")
    return dict(zip(uniq["gvkeyiid"].astype(str), uniq["sec"].astype(str)))


def sector_net_weights(weights: pd.DataFrame, sector_map: dict) -> pd.Series:
    """종목 순비중(mp_ls_weight)을 섹터로 묶은 순노출. 매핑 없는 종목은 'Unknown'."""
    w = weights.copy()
    w["sec"] = w["gvkeyiid"].astype(str).map(sector_map).fillna("Unknown")
    s = w.groupby("sec")["mp_ls_weight"].sum()
    return s[s != 0].sort_values()


# ── 백테스트 가중치 추이 / 회전율 ──────────────────────────────────────────

def load_weight_history(path: Path) -> pd.DataFrame:
    """walk_forward_weight_history.csv -> date 인덱스 x factor 컬럼 DataFrame."""
    df = pd.read_csv(path, parse_dates=["date"])
    return df.set_index("date").sort_index()


def compute_turnover(weight_history: pd.DataFrame) -> pd.Series:
    """월별 one-way 회전율 = 0.5 * sum(|w_t - w_{t-1}|). 첫 기간은 0."""
    wh = weight_history.apply(pd.to_numeric, errors="coerce").fillna(0.0).sort_index()
    turnover = 0.5 * wh.diff().abs().sum(axis=1)
    if len(turnover) > 0:
        turnover.iloc[0] = 0.0
    return turnover


def factor_style_map(factor_info_path: Path) -> dict:
    """factor_info.csv -> factorAbbreviation -> styleName 매핑."""
    df = pd.read_csv(factor_info_path)
    return dict(zip(df["factorAbbreviation"].astype(str), df["styleName"].astype(str)))


def style_weight_history(weight_history: pd.DataFrame, factor_style: dict) -> pd.DataFrame:
    """팩터 가중치 이력을 스타일별로 합산 -> date x style DataFrame (스택 영역용)."""
    wh = weight_history.apply(pd.to_numeric, errors="coerce").fillna(0.0)
    buckets: dict[str, list] = {}
    for col in wh.columns:
        buckets.setdefault(factor_style.get(str(col), "Unknown"), []).append(col)
    data = {style: wh[cols].sum(axis=1) for style, cols in buckets.items()}
    return pd.DataFrame(data, index=wh.index)
