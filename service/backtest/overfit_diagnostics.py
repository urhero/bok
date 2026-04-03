# -*- coding: utf-8 -*-
"""과적합 진단 모듈 (Step 2).

Walk-Forward 결과를 분석하여 파이프라인의 과적합 정도를 정량화한다.

지표 우선순위:
  1순위: IS-OOS Rank Correlation (팩터 순위 유지력)
  2순위: 팩터 선정 안정성 Jaccard
  3순위: Deflation Ratio (보조, 단독 판단 금지)
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from service.backtest.result_stitcher import WalkForwardResult

logger = logging.getLogger(__name__)


def calc_deflation_ratio(walk_forward_result: WalkForwardResult, full_period_cagr: float) -> dict[str, Any]:
    """IS 전체 기간 CAGR 대비 OOS CAGR의 비율을 계산한다.

    Args:
        walk_forward_result: Walk-Forward 결과.
        full_period_cagr: 전체 기간 IS simulation 모드 CAGR.

    Returns:
        deflation_ratio, oos_cagr, full_period_cagr, interpretation을 포함하는 dict.
    """
    oos_perf = walk_forward_result.calc_performance()
    oos_cagr = oos_perf["cagr"]

    result: dict[str, Any] = {
        "oos_cagr": oos_cagr,
        "full_period_cagr": full_period_cagr,
    }

    if full_period_cagr == 0:
        result["deflation_ratio"] = np.nan
        result["interpretation"] = "IS CAGR = 0 — 비율 해석 불가"
    elif full_period_cagr < 0:
        result["deflation_ratio"] = np.nan
        result["interpretation"] = f"IS CAGR 음수({full_period_cagr:.4%}), OOS CAGR({oos_cagr:.4%}) — 비율 해석 스킵"
    else:
        ratio = oos_cagr / full_period_cagr
        result["deflation_ratio"] = ratio
        if ratio > 0.6:
            result["interpretation"] = f"Deflation Ratio = {ratio:.2f} > 0.6 → 양호 (참고용)"
        elif ratio > 0.3:
            result["interpretation"] = f"Deflation Ratio = {ratio:.2f} (0.3~0.6) → 주의 (참고용)"
        else:
            result["interpretation"] = f"Deflation Ratio = {ratio:.2f} < 0.3 → 심각 (참고용)"

    return result


def calc_factor_selection_stability(is_meta_history: list[pd.DataFrame]) -> dict[str, Any]:
    """Tier 2 리밸런싱마다 선정된 팩터 목록의 안정성(Jaccard)을 측정한다.

    Args:
        is_meta_history: 각 Tier 2 리밸런싱 시점의 IS meta DataFrame 리스트.

    Returns:
        avg_jaccard, jaccard_values, interpretation을 포함하는 dict.
    """
    if len(is_meta_history) < 2:
        return {
            "avg_jaccard": np.nan,
            "jaccard_values": [],
            "interpretation": "Tier 2 리밸런싱 2회 미만 — Jaccard 계산 불가",
        }

    factor_sets = []
    for meta in is_meta_history:
        if meta is not None and "factorAbbreviation" in meta.columns:
            factor_sets.append(set(meta["factorAbbreviation"].tolist()))

    if len(factor_sets) < 2:
        return {
            "avg_jaccard": np.nan,
            "jaccard_values": [],
            "interpretation": "유효한 meta 2개 미만 — Jaccard 계산 불가",
        }

    jaccard_values = []
    for i in range(1, len(factor_sets)):
        intersection = len(factor_sets[i] & factor_sets[i - 1])
        union = len(factor_sets[i] | factor_sets[i - 1])
        jaccard = intersection / union if union > 0 else 0.0
        jaccard_values.append(jaccard)

    avg_jaccard = np.mean(jaccard_values)

    if avg_jaccard > 0.6:
        interp = f"Jaccard = {avg_jaccard:.2f} > 0.6 → 안정적"
    elif avg_jaccard > 0.4:
        interp = f"Jaccard = {avg_jaccard:.2f} (0.4~0.6) → 보통"
    else:
        interp = f"Jaccard = {avg_jaccard:.2f} < 0.4 → 불안정 (노이즈 학습 의심)"

    return {
        "avg_jaccard": avg_jaccard,
        "jaccard_values": jaccard_values,
        "interpretation": interp,
    }


def calc_is_oos_rank_correlation(walk_forward_result: WalkForwardResult) -> dict[str, Any]:
    """IS-OOS 팩터 순위 상관(Spearman)을 계산한다.

    각 Tier 2 리밸런싱 시점의 IS CAGR 순위와
    해당 OOS 기간의 실현 수익률 순위를 비교한다.

    Returns:
        avg_spearman, p_value, spearman_values, interpretation을 포함하는 dict.
    """
    meta_history = walk_forward_result.is_meta_history
    oos_history = walk_forward_result.oos_factor_returns_history

    if len(meta_history) < 1 or len(oos_history) < 1:
        return {
            "avg_spearman": np.nan,
            "p_value": np.nan,
            "spearman_values": [],
            "interpretation": "데이터 부족 — Rank Correlation 계산 불가",
        }

    # 각 Tier 2 리밸런싱 구간에 대해 Spearman 계산
    spearman_values = []
    p_values = []

    # 리밸런싱 시점 인덱스 찾기
    rebal_log = walk_forward_result.rebalance_log
    weight_rebal_indices = [i for i, (_, row) in enumerate(rebal_log.iterrows()) if row.get("is_weight_rebal", False)]

    meta_idx = 0
    for rebal_i in weight_rebal_indices:
        if meta_idx >= len(meta_history):
            break

        meta = meta_history[meta_idx]
        meta_idx += 1

        if meta is None or "factorAbbreviation" not in meta.columns or "cagr" not in meta.columns:
            continue

        is_factors = meta["factorAbbreviation"].tolist()
        is_cagr = meta["cagr"].tolist()

        # OOS 기간: 현재 리밸런싱 ~ 다음 리밸런싱 (또는 끝)
        next_rebal_i = weight_rebal_indices[weight_rebal_indices.index(rebal_i) + 1] if rebal_i != weight_rebal_indices[-1] else len(oos_history)
        oos_slice = oos_history[rebal_i:next_rebal_i]

        if not oos_slice:
            continue

        # OOS 팩터별 누적 수익률
        oos_returns_by_factor: dict[str, float] = {}
        for oos_dict in oos_slice:
            for f, r in oos_dict.items():
                oos_returns_by_factor[f] = oos_returns_by_factor.get(f, 0.0) + r

        # IS와 OOS에 공통으로 존재하는 팩터만
        common_factors = [f for f in is_factors if f in oos_returns_by_factor]
        if len(common_factors) < 3:
            continue

        is_rank = pd.Series({f: cagr for f, cagr in zip(is_factors, is_cagr) if f in common_factors}).rank(ascending=False)
        oos_rank = pd.Series({f: oos_returns_by_factor[f] for f in common_factors}).rank(ascending=False)

        # Spearman 계산
        aligned = pd.DataFrame({"is_rank": is_rank, "oos_rank": oos_rank}).dropna()
        if len(aligned) < 3:
            continue

        corr, pval = stats.spearmanr(aligned["is_rank"], aligned["oos_rank"])
        spearman_values.append(corr)
        p_values.append(pval)

    if not spearman_values:
        return {
            "avg_spearman": np.nan,
            "p_value": np.nan,
            "spearman_values": [],
            "interpretation": "Rank Correlation 계산 가능한 구간 없음",
        }

    avg_spearman = np.mean(spearman_values)
    avg_p_value = np.mean(p_values)

    if avg_spearman > 0.3:
        interp = f"Rank Corr = {avg_spearman:.2f} > 0.3 → 양의 상관 (팩터 예측력 있음)"
    elif avg_spearman > -0.1:
        interp = f"Rank Corr = {avg_spearman:.2f} ≈ 0 → IS 순위와 OOS 순위 무관"
    else:
        interp = f"Rank Corr = {avg_spearman:.2f} < 0 → 음의 상관 (과적합 의심)"

    return {
        "avg_spearman": avg_spearman,
        "p_value": avg_p_value,
        "spearman_values": spearman_values,
        "interpretation": interp,
    }


def generate_overfit_report(walk_forward_result: WalkForwardResult, full_period_cagr: float) -> dict[str, Any]:
    """과적합 종합 진단 리포트를 생성한다.

    Args:
        walk_forward_result: Walk-Forward 결과.
        full_period_cagr: 전체 기간 IS simulation 모드 CAGR.

    Returns:
        종합 진단 리포트 dict.
    """
    # 1순위: IS-OOS Rank Correlation
    rank_corr = calc_is_oos_rank_correlation(walk_forward_result)

    # 2순위: 팩터 선정 안정성
    stability = calc_factor_selection_stability(walk_forward_result.is_meta_history)

    # 3순위: Deflation Ratio
    deflation = calc_deflation_ratio(walk_forward_result, full_period_cagr)

    # OOS 성과
    mp_perf = walk_forward_result.calc_performance()
    ew_perf = walk_forward_result.calc_ew_performance()
    comparison = walk_forward_result.compare_mp_vs_ew_oos()

    n_oos = len(walk_forward_result.oos_returns)

    report = {
        # 1순위
        "is_oos_rank_spearman": rank_corr["avg_spearman"],
        "rank_corr_p_value": rank_corr["p_value"],
        "rank_corr_interpretation": rank_corr["interpretation"],
        # 2순위
        "avg_factor_jaccard": stability["avg_jaccard"],
        "jaccard_interpretation": stability["interpretation"],
        # 3순위
        "deflation_ratio": deflation.get("deflation_ratio", np.nan),
        "deflation_interpretation": deflation["interpretation"],
        # OOS 성과
        "oos_cagr": mp_perf["cagr"],
        "oos_mdd": mp_perf["mdd"],
        "oos_sharpe": mp_perf["sharpe"],
        "oos_calmar": mp_perf["calmar"],
        # OOS EW 성과
        "oos_ew_cagr": ew_perf["cagr"],
        "oos_ew_mdd": ew_perf["mdd"],
        "oos_ew_sharpe": ew_perf["sharpe"],
        # MP vs EW 비교
        "mp_vs_ew_excess_cagr": comparison["excess_cagr"],
        "mp_vs_ew_win_rate": comparison["win_rate"],
        # 경고
        "warning": (
            f"OOS 기간이 {n_oos}개월({n_oos / 12:.1f}년)이므로 특정 매크로 환경에 편향되었을 수 있음. "
            f"Deflation Ratio 수치 자체보다 IS-OOS Rank Correlation(팩터 순위 유지력)을 더 신뢰할 것."
        ),
        "limitation": (
            "본 백테스트는 factor-level에서 수행되었으며, 팩터 내부 종목 리밸런싱 비용(30bp)만 "
            "반영됨. 팩터 간 비중 변경(inter-factor rebalancing)에 따른 자산 배분 차원의 "
            "거래비용은 미반영이므로, OOS 수익률에 약간의 상방 편향이 존재할 수 있음."
        ),
    }

    return report


def print_overfit_report(report: dict[str, Any]) -> None:
    """과적합 진단 리포트를 Rich 테이블로 콘솔 출력한다."""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    console = Console()

    # 진단 지표 테이블
    table = Table(title="과적합 진단 리포트 (OOS Walk-Forward)", show_header=True)
    table.add_column("Priority", style="bold", width=8)
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_column("Interpretation")

    # 1순위
    spearman = report["is_oos_rank_spearman"]
    table.add_row(
        "1순위", "IS-OOS Rank Corr",
        f"{spearman:.4f}" if not np.isnan(spearman) else "N/A",
        report["rank_corr_interpretation"],
    )

    # 2순위
    jaccard = report["avg_factor_jaccard"]
    table.add_row(
        "2순위", "Factor Jaccard",
        f"{jaccard:.4f}" if not np.isnan(jaccard) else "N/A",
        report["jaccard_interpretation"],
    )

    # 3순위
    deflation = report["deflation_ratio"]
    table.add_row(
        "3순위", "Deflation Ratio",
        f"{deflation:.4f}" if not np.isnan(deflation) else "N/A",
        report["deflation_interpretation"],
    )

    console.print(table)

    # 성과 비교 테이블
    perf_table = Table(title="OOS 성과 비교 (MP vs. Equal-Weight)", show_header=True)
    perf_table.add_column("Metric", style="bold")
    perf_table.add_column("MP (Optimized)", justify="right")
    perf_table.add_column("EW (1/N)", justify="right")

    perf_table.add_row("CAGR", f"{report['oos_cagr']:.4%}", f"{report['oos_ew_cagr']:.4%}")
    perf_table.add_row("Excess CAGR", f"{report['mp_vs_ew_excess_cagr']:.4%}", "-")
    perf_table.add_row("MDD", f"{report['oos_mdd']:.4%}", f"{report['oos_ew_mdd']:.4%}")
    perf_table.add_row("Sharpe", f"{report['oos_sharpe']:.4f}", f"{report['oos_ew_sharpe']:.4f}")
    perf_table.add_row("Win Rate", f"{report['mp_vs_ew_win_rate']:.2%}", "-")

    console.print(perf_table)

    # 경고 패널
    console.print(Panel(report["warning"], title="경고", style="yellow"))
    console.print(Panel(report["limitation"], title="한계점", style="dim"))
