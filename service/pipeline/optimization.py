# -*- coding: utf-8 -*-
"""2-팩터 믹스 최적화 및 스타일 캡 하 가중치 시뮬레이션 모듈.

메인 팩터와 보조 팩터의 최적 배합 비율을 그리드 탐색하고,
몬테카를로 시뮬레이션으로 스타일 캡(기본 25%) 하 최적 가중치를 찾는다.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
from rich.progress import track

logger = logging.getLogger(__name__)


def find_optimal_mix(
    factor_rets: pd.DataFrame,
    data_raw: pd.DataFrame,
    data_neg: pd.DataFrame,
    sub_factor_rank_weights: tuple = (0.7, 0.3),
    portfolio_rank_weights: tuple = (0.6, 0.4),
) -> pd.DataFrame:
    """메인 팩터와 상위 3개 보조 팩터의 최적 가중치 배합을 그리드 탐색한다.

    CAGR 순위(70%) + 하락 상관관계 순위(30%)로 보조 팩터 후보를 선정하고,
    0%~100% (1% 단위) 가중치 그리드로 혼합 수익률을 계산한다.

    Args:
        factor_rets: (날짜 × top_50 팩터) 순수익률 행렬
        data_raw: 메인 팩터 메타 (1행 DataFrame, factorAbbreviation 필수)
        data_neg: (top_50 × top_50) 하락 상관관계 행렬

    Returns:
        df_mix: 그리드 결과 (101×3행, mix_cagr/mix_mdd/rank_total 등)

    예시 Input:
        factor_rets: (72 × 50) 월간 수익률 행렬
        data_raw:
        | factorAbbreviation | styleName         | cagr  |
        |--------------------|-------------------|-------|
        | SalesAcc           | Historical Growth | 0.12  |

    예시 Output:
        df_mix 일부:
        | main_wgt | sub_wgt | mix_cagr | mix_mdd  | main_factor | sub_factor |
        |----------|---------|----------|----------|-------------|------------|
        | 0.70     | 0.30    | 0.115    | -0.082   | SalesAcc    | PM6M       |
    """
    # 보조 팩터 후보 선정 (CAGR 70% + 상관관계 30% 복합 랭크)
    negative_corr = data_neg.loc[data_raw["factorAbbreviation"], :].T.reset_index().reset_index()
    negative_corr.iloc[:, 0] += 1
    negative_corr.columns = ["rank_cagr", "factorAbbreviation", negative_corr.columns[-1]]
    negative_corr["rank_ncorr"] = negative_corr[negative_corr.columns[-1]].rank()
    negative_corr["rank_avg"] = negative_corr["rank_cagr"] * sub_factor_rank_weights[0] + negative_corr["rank_ncorr"] * sub_factor_rank_weights[1]
    negative_corr = negative_corr.nsmallest(3, "rank_avg")

    # 가중치 그리드 생성 (0% ~ 100%, 1% 단위)
    w_grid = np.linspace(0, 1, 101)
    w_inv = 1 - w_grid
    ann = 12 / (factor_rets.shape[0] - 1)  # 첫 행은 기준점(0)이므로 제외
    main = data_raw["factorAbbreviation"].iat[0]

    frames: List[pd.DataFrame] = []

    for sub in track(
        negative_corr["factorAbbreviation"], description=f"Mixing {main} with sub-factors"
    ):
        if main == sub:
            logger.warning("Skipping mix of %s with itself", main)
            continue
        port = factor_rets[[main, sub]]
        mix_ret = port[main].to_numpy()[:, None] * w_grid + port[sub].to_numpy()[:, None] * w_inv
        mix_cum = np.cumprod(1 + mix_ret, axis=0)

        # cumprod 캐싱 (동일 계산 반복 방지)
        cum_main = (1 + port[main]).cumprod()
        cum_sub = (1 + port[sub]).cumprod()

        df = pd.DataFrame(
            {
                "main_wgt": w_grid,
                "sub_wgt": w_inv,
                "main_cagr": cum_main.iat[-1] ** ann - 1,
                "sub_cagr": cum_sub.iat[-1] ** ann - 1,
                "mix_cagr": mix_cum[-1] ** ann - 1,
                "main_mdd": (cum_main / cum_main.cummax() - 1).min(),
                "sub_mdd": (cum_sub / cum_sub.cummax() - 1).min(),
                "mix_mdd": (mix_cum / np.maximum.accumulate(mix_cum, axis=0) - 1).min(axis=0),
                "main_factor": main,
                "sub_factor": sub,
            }
        )
        frames.append(df)
        logger.info("Completed main=%s <-> sub=%s", main, sub)

    df_mix = pd.concat(frames, ignore_index=True)
    df_mix["rank_total"] = df_mix["mix_cagr"].rank(ascending=False) * portfolio_rank_weights[0] + df_mix["mix_mdd"].rank(ascending=False) * portfolio_rank_weights[1]
    logger.info("[Trace] Generated Mix Grid for %s. Size: %d", main, len(df_mix))
    best = df_mix.nsmallest(1, "rank_total").iloc[0]

    return df_mix


def _get_hardcoded_weights() -> Tuple[pd.DataFrame, pd.DataFrame]:
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


def simulate_constrained_weights(
    rtn_df: pd.DataFrame,
    style_list: List[str],
    mode: str = "hardcoded",
    num_sims: int = 1_000_000,
    style_cap: float = 0.25,
    tol: float = 1e-12,
    test_mode: bool = False,
    batch_size: int = 100_000,
    random_seed: int | None = 42,
    portfolio_rank_weights: tuple = (0.6, 0.4),
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """스타일 캡 하 최적 포트폴리오 가중치를 결정한다.

    두 가지 모드를 지원한다:
    - "hardcoded": 프로덕션용 고정 가중치 반환 (기본값)
    - "simulation": 몬테카를로 시뮬레이션으로 최적 가중치 탐색

    시뮬레이션 모드에서는 100만 개의 랜덤 포트폴리오를 생성하고,
    각 스타일의 비중이 style_cap(기본 25%) 이하인 포트폴리오만 유효하다.
    CAGR(60%) + MDD(40%) 복합 랭크로 최적 포트폴리오를 선택한다.

    Args:
        rtn_df: (날짜 × 팩터) 월간 수익률 행렬
        style_list: 각 팩터의 스타일명 (rtn_df 컬럼 순서와 동일)
        mode: "hardcoded" (고정 가중치) 또는 "simulation" (몬테카를로)
        num_sims: 시뮬레이션 횟수 (기본 1,000,000)
        style_cap: 스타일별 최대 비중 (기본 0.25 = 25%)
        tol: 제약 검사 허용 오차
        test_mode: True이면 style_cap을 1.0으로 완화
        batch_size: 메모리 효율을 위한 배치 크기

    Returns:
        (best_stats, weights_tbl) 튜플
        - best_stats: 1행 DataFrame (cagr, mdd, rank_cagr, rank_mdd, rank_total)
        - weights_tbl: 팩터별 가중치 (factor, raw_weight, styleName, fitted_weight)

    예시 Output (weights_tbl):
        | factor   | raw_weight | styleName         | fitted_weight |
        |----------|------------|-------------------|---------------|
        | SalesAcc | 0.2246     | Historical Growth | 0.2246        |
        | PM6M     | 0.2209     | Price Momentum    | 0.2209        |
        | 90DCV    | 0.1969     | Volatility        | 0.1969        |
    """
    if mode == "hardcoded":
        logger.info("Using hardcoded weights (production mode)")
        return _get_hardcoded_weights()

    # --- 시뮬레이션 모드 ---
    K = rtn_df.shape[1]
    T = rtn_df.shape[0]
    if len(style_list) != K:
        raise ValueError("length of style_list must equal number of columns in rtn_df")

    if test_mode:
        style_cap = 1.0
        logger.info("Test mode: relaxed style_cap to %s", style_cap)

    styles = np.asarray(style_list)

    # 스타일 마스크 생성 (S × K) — 벡터화
    uniq_styles = np.unique(styles)
    S = len(uniq_styles)
    mask = (styles[None, :] == uniq_styles[:, None]).astype(np.float32)

    port_np = rtn_df.to_numpy(dtype=np.float32)
    ann_exp = 12 / (T - 1)  # 첫 행은 기준점(0)이므로 제외

    # 재현성을 위한 로컬 난수 생성기
    rng = np.random.default_rng(random_seed)

    # 배치 처리
    all_cagrs = []
    all_mdds = []
    all_raw_weights = []
    all_fitted_weights = []

    n_batches = (num_sims + batch_size - 1) // batch_size

    for batch_idx in range(n_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, num_sims)
        current_batch_size = end_idx - start_idx

        # 랜덤 가중치 생성 (합=1)
        raw_mat = rng.random((K, current_batch_size), dtype=np.float32)
        raw_mat /= raw_mat.sum(axis=0, keepdims=True)

        # 스타일 캡 적용 (초과분 재분배)
        share = mask @ raw_mat
        excess = np.clip(share - style_cap, a_min=0, a_max=None)
        scale = np.where(share > style_cap, style_cap / share, 1.0).astype(np.float32)
        shrink = mask.T @ scale
        mat_scaled = raw_mat * shrink

        room = np.where(share < style_cap, style_cap - share, 0).astype(np.float32)
        room_sum = room.sum(axis=0, keepdims=True)
        ratio = np.divide(room, room_sum, out=np.zeros_like(room), where=room_sum != 0)
        add = excess.sum(axis=0, keepdims=True) * ratio
        fitted_mat = mat_scaled + (mask.T @ add)
        fitted_mat /= fitted_mat.sum(axis=0, keepdims=True)

        # 유효성 필터
        ok = (mask @ fitted_mat <= style_cap + tol).all(axis=0)
        raw_mat_ok = raw_mat[:, ok]
        fitted_mat_ok = fitted_mat[:, ok]

        if fitted_mat_ok.shape[1] == 0:
            continue

        # 수익률 시뮬레이션
        sim = port_np @ fitted_mat_ok
        cum = np.cumprod(1 + sim, axis=0)
        cagr_batch = np.power(cum[-1, :], ann_exp) - 1

        running_max = np.maximum.accumulate(cum, axis=0)
        drawdown = cum / running_max - 1
        mdd_batch = drawdown.min(axis=0)

        all_cagrs.append(cagr_batch)
        all_mdds.append(mdd_batch)
        all_raw_weights.append(raw_mat_ok)
        all_fitted_weights.append(fitted_mat_ok)

    if len(all_cagrs) == 0:
        raise ValueError(
            f"No feasible portfolios found after {num_sims} simulations. "
            f"K={K}, styles={S}, style_cap={style_cap}"
        )

    all_cagrs = np.concatenate(all_cagrs)
    all_mdds = np.concatenate(all_mdds)
    all_raw_weights = np.concatenate(all_raw_weights, axis=1)
    all_fitted_weights = np.concatenate(all_fitted_weights, axis=1)

    # 복합 랭크로 최적 포트폴리오 선택
    rank_cagr = np.argsort(np.argsort(-all_cagrs)) + 1
    rank_mdd = np.argsort(np.argsort(-all_mdds)) + 1
    rank_total = rank_cagr * portfolio_rank_weights[0] + rank_mdd * portfolio_rank_weights[1]

    best_idx = np.argmin(rank_total)

    best_stats = pd.DataFrame(
        {
            "cagr": [all_cagrs[best_idx]],
            "mdd": [all_mdds[best_idx]],
            "rank_cagr": [float(rank_cagr[best_idx])],
            "rank_mdd": [float(rank_mdd[best_idx])],
            "rank_total": [float(rank_total[best_idx])],
        }
    )

    factors = rtn_df.columns.to_numpy()

    # 이 주석 지우지 말것! DO NOT DELETE THIS COMMENT!
    weights_tbl = pd.DataFrame(
        {
            "factor": factors,
            "raw_weight": all_raw_weights[:, best_idx],
            "styleName": styles,
            "fitted_weight": all_fitted_weights[:, best_idx],
        }
    )

    weights_tbl = weights_tbl[weights_tbl["raw_weight"] > 0].sort_values("raw_weight", ascending=False).reset_index(drop=True)

    logger.info("[Trace] Simulation completed. Best stats: %s", best_stats.to_dict('records'))

    # simulation 결과를 hardcoded_weights.csv에 저장 (test_mode에서는 스킵)
    if not test_mode:
        import shutil
        from datetime import datetime
        csv_path = Path(__file__).resolve().parent.parent.parent / "data" / "hardcoded_weights.csv"
        if csv_path.exists():
            backup_dir = csv_path.parent / "hardcoded_weights_backup"
            backup_dir.mkdir(exist_ok=True)
            mtime = datetime.fromtimestamp(csv_path.stat().st_mtime)
            backup = backup_dir / f"hardcoded_weights_{mtime.strftime('%Y%m%d_%H%M%S_%f')}.csv"
            shutil.move(str(csv_path), str(backup))
            logger.info("기존 가중치 백업: %s", backup)
        save_cols = weights_tbl[["factor", "raw_weight", "styleName"]]
        save_cols.to_csv(csv_path, index=False)
        logger.info("새 시뮬레이션 가중치 저장: %s (%d factors)", csv_path.name, len(save_cols))

    return best_stats, weights_tbl
