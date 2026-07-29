# -*- coding: utf-8 -*-
"""스타일 캡 하 가중치 최적화 모듈.

스타일 캡(기본 25%) 제약 하에서 팩터별 가중치를 결정한다.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _get_hardcoded_weights() -> tuple[pd.DataFrame, pd.DataFrame]:
    """프로덕션용 고정 가중치를 반환한다.

    ~2026-01 포트폴리오까지 적용. Valuation 강제로 4%로 내림.
    (이 주석 지우지 말것! DO NOT DELETE THIS COMMENT!)
    """
    best_stats = pd.DataFrame(
        {c: [np.nan] for c in ["cagr", "mdd", "rank_cagr", "rank_mdd", "rank_total"]}
    )

    # 가중치를 CSV에서 로드 (과거 버전은 git history 참조)
    csv_path = Path(__file__).resolve().parent.parent.parent / "data" / "hardcoded_weights.csv"
    weights_tbl = pd.read_csv(csv_path, float_precision="round_trip")
    weights_tbl["fitted_weight"] = weights_tbl["raw_weight"]
    weights_tbl = weights_tbl[weights_tbl["raw_weight"] > 0].sort_values("raw_weight", ascending=False).reset_index(drop=True)

    return best_stats, weights_tbl


def _redistribute_to_cap(
    x: np.ndarray,
    styles_arr: np.ndarray,
    uniq_styles: np.ndarray,
    style_cap: float,
    tol: float,
) -> np.ndarray:
    """스타일 합이 cap 을 넘지 않도록 비례 축소 + 재정규화 (수렴까지 최대 10회).

    x 는 캡을 적용할 단위의 벡터 (weight basis 면 비중, risk basis 면
    리스크 예산 w*sigma). 연산 순서는 기존 인라인 루프와 동일 (byte 보존).
    """
    for _ in range(10):
        for s in uniq_styles:
            mask_s = styles_arr == s
            style_w = x[mask_s].sum()
            if style_w > style_cap + tol:
                x[mask_s] *= style_cap / style_w
        x /= x.sum()
        if all(x[styles_arr == s].sum() <= style_cap + tol for s in uniq_styles):
            break
    # 10회 내 미수렴 시 위반 스타일 보고 (float32 오차 허용 1e-6)
    violations = {
        s: float(x[styles_arr == s].sum())
        for s in uniq_styles if x[styles_arr == s].sum() > style_cap + 1e-6
    }
    if violations:
        logger.warning(
            "style_cap %.2f violated after redistribution: %s",
            style_cap, {s: round(v, 4) for s, v in violations.items()},
        )
    return x


def _equal_weight_allocation(
    rtn_df: pd.DataFrame,
    style_list: list[str],
    style_cap: float,
    tol: float,
    test_mode: bool,
    base_weights: np.ndarray | None = None,
    cap_scale: np.ndarray | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """초기 가중(기본 1/N, base_weights 지정 시 그 값) + 스타일 캡 재분배.

    cap_scale 지정 시(risk basis) 캡 재분배를 x = w * cap_scale (리스크 예산)
    공간에서 수행한 뒤 명목비중으로 환산한다. None 이면 기존 명목비중 기준 그대로.
    """
    n_factors = rtn_df.shape[1]
    factors = rtn_df.columns.to_numpy()
    styles_arr = np.asarray(style_list)

    if base_weights is None:
        w = np.ones(n_factors, dtype=np.float32) / n_factors
    else:
        w = base_weights.astype(np.float32).copy()
        w /= w.sum()

    # 스타일 캡 재분배 (수렴까지 반복)
    uniq_styles = np.unique(styles_arr)
    if not test_mode:
        # feasibility 가드: n_styles x cap < 100% 면 제약 자체가 불가능
        # (예: 스타일 3개 x 25% = 75%). 동작은 기존과 동일 - 경고만 남긴다.
        if len(uniq_styles) * style_cap < 1.0 - tol:
            logger.warning(
                "style_cap %.2f infeasible with %d styles (max %.0f%% < 100%%) - constraint will be violated",
                style_cap, len(uniq_styles), len(uniq_styles) * style_cap * 100,
            )
        if cap_scale is None:
            w = _redistribute_to_cap(w, styles_arr, uniq_styles, style_cap, tol)
        else:
            # risk basis: 리스크 예산 공간에서 캡 적용 후 명목비중으로 환산
            x = (w * cap_scale.astype(np.float32))
            x /= x.sum()
            x = _redistribute_to_cap(x, styles_arr, uniq_styles, style_cap, tol)
            w = x / cap_scale.astype(np.float32)
            w /= w.sum()
            # 진단: 명목비중(notional) 기준 캡 위반 여부 (규제 요건이 명목비중 기준일 경우 참고)
            notional = {
                s: float(w[styles_arr == s].sum())
                for s in uniq_styles if w[styles_arr == s].sum() > style_cap + 1e-6
            }
            if notional:
                logger.info(
                    "style_cap(risk basis): notional share exceeds %.2f: %s",
                    style_cap, {s: round(v, 4) for s, v in notional.items()},
                )

    weights_tbl = pd.DataFrame({
        "factor": factors,
        "raw_weight": w,
        "styleName": styles_arr,
        "fitted_weight": w,
    })

    # CAGR/MDD 계산 (기록용)
    port_np = rtn_df.to_numpy(dtype=np.float32)
    n_months = port_np.shape[0]
    sim = port_np @ w
    cum = np.cumprod(1 + sim)
    ann_exp = 12 / max(n_months - 1, 1)
    cagr_val = float(cum[-1] ** ann_exp - 1)
    mdd_val = float((cum / np.maximum.accumulate(cum) - 1).min())

    best_stats = pd.DataFrame({
        "cagr": [cagr_val], "mdd": [mdd_val],
        "rank_cagr": [np.nan], "rank_mdd": [np.nan], "rank_total": [np.nan],
    })

    logger.info("Equal-weight allocation: %d factors, CAGR=%.4f, MDD=%.4f", n_factors, cagr_val, mdd_val)
    return best_stats, weights_tbl


def _solve_erc_ccd(cov: np.ndarray, max_sweeps: int = 500, tol_w: float = 1e-12) -> np.ndarray:
    """정식 ERC 해 (Spinu 볼록형의 CCD 반복, 2026-07-30 교체).

    min (1/2) w'Σw - λ Σ ln w_i 의 1계 조건 w_i(Σw)_i = λ (모든 i 동일) 가 곧 ERC.
    좌표별 닫힌해 w_i = (-b_i + sqrt(b_i^2 + 4 σ_ii λ)) / (2 σ_ii), b_i = Σ_{j≠i} σ_ij w_j.
    sqrt 항이 |b_i| 보다 항상 크므로 **음의 상관(b_i<0)에서도 양수 비중이 보장**되고,
    그런 팩터는 (Σw)_i 가 작아 오히려 큰 비중을 받는다 (분산 재료 우대 — 구 곱셈
    반복법이 이들을 0으로 붕괴시키던 결함의 교정). PD cov 에서 해는 유일(스케일 제외).
    """
    n = cov.shape[0]
    if n == 1:
        return np.array([1.0])
    diag = np.maximum(np.diag(cov), 1e-18)
    lam = float(np.mean(diag)) / n  # 스케일 파라미터 (최종 정규화 후 결과와 무관)
    w = 1.0 / np.sqrt(diag)
    for _ in range(max_sweeps):
        w_prev = w.copy()
        for i in range(n):
            b = cov[i] @ w - diag[i] * w[i]
            w[i] = (-b + np.sqrt(b * b + 4.0 * diag[i] * lam)) / (2.0 * diag[i])
        # 정규화는 수렴 후 1회 (반복 중 하면 고정점 조건 w_i(Σw)_i=λ 가 틀어짐)
        if np.abs(w - w_prev).max() < tol_w * max(1.0, np.abs(w).max()):
            break
    return w / w.sum()


def blend_deploy_weights(
    target: dict[str, float],
    prev: dict[str, float] | None,
    step: float,
) -> dict[str, float]:
    """부분 조정 배포: prev 에서 target 으로 step 만큼만 이동 후 합 1 재정규화.

    step>=1 또는 prev 없음 -> target 그대로 (기존 동작 보존).
    월간 신호를 유지하면서 트레이드 크기를 줄여 실측 비용을 절감한다
    (2026-07-29 MP-level 실측: step 0.5 에서 net Sharpe 0.448->0.564).
    """
    if step >= 1.0 or not prev:
        return dict(target)
    keys = sorted(set(target) | set(prev))
    blended = {f: step * target.get(f, 0.0) + (1.0 - step) * prev.get(f, 0.0) for f in keys}
    total = sum(blended.values())
    return {f: w / total for f, w in blended.items() if w > 1e-10}


def optimize_constrained_weights(
    rtn_df: pd.DataFrame,
    style_list: list[str],
    mode: str = "equal_weight",
    style_cap: float = 0.25,
    tol: float = 1e-12,
    test_mode: bool = False,
    style_cap_basis: str = "weight",
    erw_vol_window: int | None = None,
    erc_shrinkage: float = 0.5,
    erc_shrink_target: str = "diag",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """스타일 캡 하 포트폴리오 가중치를 결정한다.

    현재 "최적화"라는 이름과 달리, 공분산/리스크 모델 기반의 진짜 최적화는
    제거됐다 (커밋 8dfb64e). 두 모드 모두 학습되는 가중치 없음.

    세 가지 모드를 지원한다:
    - "equal_weight": 1/N 동일가중 + 스타일 캡 재분배 (config.py 기본값;
      backtest 모드에서 "hardcoded"를 주면 자동으로 이 모드로 변환됨)
    - "equal_risk_weight": 팩터 IS 변동성 반비례 가중 + 스타일 캡 재분배 (실험용,
      docs/experiments 참조)
    - "hardcoded": 프로덕션용 고정 가중치 CSV (data/hardcoded_weights.csv) 반환

    Args:
        rtn_df: (날짜 x 팩터) 월간 수익률 행렬
        style_list: 각 팩터의 스타일명 (rtn_df 컬럼 순서와 동일)
        mode: "equal_weight"(기본, config.py 기본값) / "hardcoded"
        style_cap: 스타일별 최대 비중 (기본 0.25 = 25%)
        tol: 제약 검사 허용 오차
        test_mode: True이면 style_cap을 1.0으로 완화

    Returns:
        (best_stats, weights_tbl) 튜플
        - best_stats: 1행 DataFrame (cagr, mdd, rank_cagr, rank_mdd, rank_total)
        - weights_tbl: 팩터별 가중치 (factor, raw_weight, styleName, fitted_weight)
    """
    if mode == "hardcoded":
        logger.info("Using hardcoded weights (production mode)")
        return _get_hardcoded_weights()

    if mode == "equal_weight":
        return _equal_weight_allocation(rtn_df, style_list, style_cap, tol, test_mode)

    if mode == "equal_risk_weight":
        # 팩터 IS 월간 수익률 변동성 반비례 가중. 첫 행(기준점 0) 제외는
        # compute_rank_score 의 monthly_rets = iloc[1:] 관례와 동일.
        # erw_vol_window 지정 시 최근 N개월 실현 변동성만 사용 (내부 vol 타이밍:
        # 총 비중 합 1 불변 — 포트폴리오 배수가 아니라 팩터 믹스 재배분).
        vol_src = rtn_df.iloc[1:]
        if erw_vol_window and len(vol_src) > erw_vol_window:
            vol_src = vol_src.iloc[-erw_vol_window:]
        vol = vol_src.std().to_numpy(dtype=np.float64)
        # ponytail: 무분산 팩터 폭주 방지 하한. zero-filter 가 상류에서 대부분 거르므로
        # 실전에서 밟힐 일은 드물다.
        vol = np.maximum(vol, 1e-6)
        base = 1.0 / vol
        # style_cap_basis="risk": 캡을 비중이 아닌 리스크 예산(w*sigma) 기준으로 적용
        cap_scale = vol if style_cap_basis == "risk" else None
        return _equal_weight_allocation(
            rtn_df, style_list, style_cap, tol, test_mode,
            base_weights=base, cap_scale=cap_scale,
        )

    if mode == "min_var":
        # 롱온리 최소분산: min w'Σw, w>=0, Σw=1 (2026-07-30 — 붕괴 ERC 가 우연히
        # 근사하던 저변동·저상관 팩터 틸트의 의도된 설계 버전. cov 는 erc 와 동일 수축)
        vol_src = rtn_df.iloc[1:]
        if erw_vol_window and len(vol_src) > erw_vol_window:
            vol_src = vol_src.iloc[-erw_vol_window:]
        cov = vol_src.cov().to_numpy(dtype=np.float64)
        cov = (1.0 - erc_shrinkage) * cov + erc_shrinkage * np.diag(np.diag(cov))
        n = cov.shape[0]
        from scipy.optimize import minimize
        res = minimize(
            lambda w: w @ cov @ w, np.full(n, 1.0 / n),
            jac=lambda w: 2.0 * (cov @ w), method="SLSQP",
            bounds=[(0.0, 1.0)] * n,
            constraints=[{"type": "eq", "fun": lambda w: w.sum() - 1.0}],
            options={"maxiter": 300, "ftol": 1e-12},
        )
        w = np.maximum(res.x, 0.0)
        w = w / w.sum() if w.sum() > 0 else np.full(n, 1.0 / n)
        return _equal_weight_allocation(
            rtn_df, style_list, style_cap, tol, test_mode, base_weights=w,
        )

    if mode == "erc":
        # Equal Risk Contribution: 팩터 간 상관을 반영해 리스크 기여를 균등화.
        # 1/sigma(ERW)의 상관 무시 한계 보완 — 총 비중 합 1 불변 (배수 아님).
        # cov 는 진단 대각 50% 수축(shrinkage)으로 추정 노이즈 완화.
        vol_src = rtn_df.iloc[1:]
        if erw_vol_window and len(vol_src) > erw_vol_window:
            vol_src = vol_src.iloc[-erw_vol_window:]
        cov = vol_src.cov().to_numpy(dtype=np.float64)
        # erc_shrinkage: 수축 비율 (0=표본 cov 그대로).
        # erc_shrink_target: "diag"=무상관 타깃(1/sigma 방향) /
        #   "cc"=상수상관 타깃(Ledoit-Wolf constant-correlation 계열 — 평균 상관 보존)
        sd = np.sqrt(np.maximum(np.diag(cov), 1e-18))
        if erc_shrink_target == "cc":
            corr = cov / np.outer(sd, sd)
            n_ = corr.shape[0]
            rho_bar = (corr.sum() - n_) / (n_ * (n_ - 1)) if n_ > 1 else 0.0
            target = rho_bar * np.outer(sd, sd)
            np.fill_diagonal(target, sd ** 2)
        else:
            target = np.diag(np.diag(cov))
        cov = (1.0 - erc_shrinkage) * cov + erc_shrinkage * target
        w = _solve_erc_ccd(cov)
        return _equal_weight_allocation(
            rtn_df, style_list, style_cap, tol, test_mode, base_weights=w,
        )

    raise ValueError(
        f"Unknown optimization mode: {mode!r}. Use 'hardcoded', 'equal_weight', 'equal_risk_weight' or 'erc'."
    )
