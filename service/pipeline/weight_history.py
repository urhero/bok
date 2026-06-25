# -*- coding: utf-8 -*-
"""Production mp 명령의 factor weight history 관리.

Absolute-step smoothing 을 production 에서도 적용하기 위해
이전 mp 실행의 factor weights 를 별도 디렉토리에 저장 / 로딩한다.

설계 원리:
- 첫 mp 실행: prev 없음 -> raw weights 그대로 (smoothing skip)
- 두번째 이상 실행: 직전 가장 최근 history 로딩 -> absolute-step 블렌딩
- history 저장은 smoothing 적용 결과
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def _latest_history_file(
    history_dir: Path, prefix: str, current_end_date: str | pd.Timestamp,
) -> tuple[Path | None, str | None]:
    """{prefix}_{date}.csv 중 current_end_date 미만에서 가장 최근 파일을 찾는다.

    Returns:
        (파일 경로, 날짜 문자열) 또는 (None, None).
    """
    history_dir = Path(history_dir)
    if not history_dir.exists():
        return None, None

    cutoff = pd.Timestamp(current_end_date)
    candidates: list[tuple[pd.Timestamp, Path]] = []
    for f in history_dir.glob(f"{prefix}_*.csv"):
        try:
            ddt_str = f.stem.replace(f"{prefix}_", "")
            d = pd.Timestamp(ddt_str)
        except (ValueError, TypeError):
            logger.warning("weight_history: 파싱 실패 %s", f.name)
            continue
        if d < cutoff:
            candidates.append((d, f))

    if not candidates:
        return None, None

    candidates.sort()
    _, latest_path = candidates[-1]
    return latest_path, latest_path.stem.replace(f"{prefix}_", "")


def load_prev_factor_weights(
    history_dir: Path, current_end_date: str | pd.Timestamp,
) -> tuple[dict[str, float] | None, str | None]:
    """가장 최근 (current_end_date 미만의) factor weights 와 해당 날짜 문자열을 반환.

    Args:
        history_dir: factor weight history 디렉토리.
        current_end_date: 현재 mp 실행의 end_date (이 시점 이전 history 만 검색).

    Returns:
        ({factor_abbr: weight}, date_str) 튜플.
        history 없을 시 (None, None).
    """
    latest_path, ddt_str = _latest_history_file(history_dir, "factor_weights", current_end_date)
    if latest_path is None:
        return None, None

    df = pd.read_csv(latest_path)
    weights = dict(zip(df["factor"].astype(str), df["weight"].astype(float)))
    logger.info(
        "weight_history: prev 로딩 (%s, %d factors)",
        latest_path.name, len(weights),
    )
    return weights, ddt_str


def load_prev_selection(
    history_dir: Path, current_end_date: str | pd.Timestamp,
) -> tuple[set[str] | None, str | None]:
    """직전 회차의 '선정' factor 집합 (raw_weight > 0) 과 날짜 문자열을 반환.

    선정 히스테리시스의 incumbency 입력. factor_styles_{date}.csv 의
    raw_weight (optimizer 목표, 스무딩 전) > 0 인 factor 만 취한다 —
    배포 가중치(factor_weights) 키를 쓰면 스무딩 청산 중인 탈락 factor 가
    incumbent 로 잘못 잡히므로 raw 기준이 정확하다.

    Returns:
        ({factor_abbr}, date_str) 튜플. history 없을 시 (None, None).
    """
    latest_path, ddt_str = _latest_history_file(history_dir, "factor_styles", current_end_date)
    if latest_path is None:
        return None, None

    df = pd.read_csv(latest_path)
    selected = set(df.loc[df["raw_weight"] > 1e-12, "factor"].astype(str))
    logger.info(
        "weight_history: prev selection 로딩 (%s, %d factors)",
        latest_path.name, len(selected),
    )
    return selected, ddt_str


def save_factor_weights(
    history_dir: Path, end_date: str | pd.Timestamp,
    weights: dict[str, float],
) -> Path:
    """현재 mp 실행의 factor weights 를 history 에 저장.

    Args:
        history_dir: 저장 디렉토리 (없으면 생성).
        end_date: 현재 mp 실행의 end_date.
        weights: {factor_abbr: weight} dict.

    Returns:
        저장된 파일 경로.
    """
    history_dir = Path(history_dir)
    history_dir.mkdir(parents=True, exist_ok=True)

    ddt_str = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    out_path = history_dir / f"factor_weights_{ddt_str}.csv"

    df = pd.DataFrame(
        [{"factor": f, "weight": w} for f, w in sorted(weights.items())]
    )
    df.to_csv(out_path, index=False)
    logger.info("weight_history: 저장 %s (%d factors)", out_path.name, len(weights))
    return out_path


def _build_factor_style_df(
    raw_weights: dict[str, float],
    prev_weights: dict[str, float] | None,
    new_weights: dict[str, float],
    style_map: dict[str, str],
) -> pd.DataFrame:
    """factor union DataFrame 을 만든다 (save 함수들이 공유).

    Args:
        raw_weights: 이번 회차 optimizer 산출 가중치.
        prev_weights: 직전 회차 배포 가중치 (None 이면 prev_weight 컬럼 NaN).
        new_weights: 실제 배포될 최종 가중치 (smoothing 적용 결과).
        style_map: {factor_abbr: style_name}. 매핑 실패 시 "(unmapped)".

    Returns:
        columns = [factor, style, raw_weight, prev_weight, new_weight, weight_within_style]
        정렬: (style asc, new_weight desc).
    """
    factors = sorted(set(raw_weights) | set(new_weights) | set(prev_weights or {}))

    data = {
        "factor": factors,
        "style": [style_map.get(f, "(unmapped)") for f in factors],
        "raw_weight": [float(raw_weights.get(f, 0.0)) for f in factors],
        "prev_weight": (
            [float(prev_weights.get(f, 0.0)) for f in factors]
            if prev_weights is not None
            else [float("nan")] * len(factors)
        ),
        "new_weight": [float(new_weights.get(f, 0.0)) for f in factors],
    }

    df = pd.DataFrame(data)

    style_totals = df.groupby("style")["new_weight"].transform("sum")
    df["weight_within_style"] = df["new_weight"] / style_totals.where(style_totals != 0, other=1.0)
    df.loc[style_totals == 0, "weight_within_style"] = 0.0

    df = df.sort_values(["style", "new_weight"], ascending=[True, False]).reset_index(drop=True)
    return df


def save_factor_styles(
    history_dir: Path,
    end_date: str | pd.Timestamp,
    raw_weights: dict[str, float],
    prev_weights: dict[str, float] | None,
    new_weights: dict[str, float],
    style_map: dict[str, str],
) -> Path:
    """factor x style 분해 + raw/prev/new 가중치를 CSV 로 저장.

    Args:
        history_dir: 저장 디렉토리 (없으면 생성).
        end_date: 현재 mp 실행의 end_date.
        raw_weights: optimizer 산출 가중치 (smoothing 전).
        prev_weights: 직전 회차 배포 가중치, 또는 None.
        new_weights: 실제 배포 가중치 (smoothing 적용 결과, 또는 raw).
        style_map: {factor_abbr: style_name}.

    Returns:
        저장된 파일 경로.
    """
    history_dir = Path(history_dir)
    history_dir.mkdir(parents=True, exist_ok=True)

    df = _build_factor_style_df(raw_weights, prev_weights, new_weights, style_map)

    ddt_str = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    out_path = history_dir / f"factor_styles_{ddt_str}.csv"
    df.to_csv(out_path, index=False)
    logger.info("weight_history: factor_styles saved %s (%d rows)", out_path.name, len(df))
    return out_path


def save_style_totals(
    history_dir: Path,
    end_date: str | pd.Timestamp,
    raw_weights: dict[str, float],
    prev_weights: dict[str, float] | None,
    new_weights: dict[str, float],
    style_map: dict[str, str],
) -> Path:
    """style 단위 합계 + factor 개수/목록을 CSV 로 저장.

    Args:
        history_dir, end_date, raw_weights, prev_weights, new_weights, style_map:
            save_factor_styles 와 동일.

    Returns:
        저장된 파일 경로.
    """
    history_dir = Path(history_dir)
    history_dir.mkdir(parents=True, exist_ok=True)

    factor_df = _build_factor_style_df(raw_weights, prev_weights, new_weights, style_map)

    # factor_df 는 이미 (style asc, new_weight desc) 정렬됨 -> factors 문자열 순서 유지
    grouped = factor_df.groupby("style", sort=False)
    rows = []
    for style, sub in grouped:
        active = sub[sub["new_weight"] > 0]
        row = {
            "style": style,
            "raw_weight": sub["raw_weight"].sum(),
            "prev_weight": (
                sub["prev_weight"].sum() if not sub["prev_weight"].isna().all() else float("nan")
            ),
            "new_weight": sub["new_weight"].sum(),
            "delta": (
                sub["new_weight"].sum() - sub["prev_weight"].sum()
                if not sub["prev_weight"].isna().all() else float("nan")
            ),
            "factor_count": int(len(active)),
            "factors": ";".join(active["factor"].tolist()),
        }
        rows.append(row)
    df = pd.DataFrame(rows).sort_values("new_weight", ascending=False).reset_index(drop=True)

    ddt_str = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    out_path = history_dir / f"style_totals_{ddt_str}.csv"
    df.to_csv(out_path, index=False)
    logger.info("weight_history: style_totals saved %s (%d styles)", out_path.name, len(df))
    return out_path


