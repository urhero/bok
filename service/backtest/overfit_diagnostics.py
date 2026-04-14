# -*- coding: utf-8 -*-
"""과적합 진단 모듈 (Step 2).

Walk-Forward 결과를 분석하여 파이프라인의 과적합 정도를 정량화한다.

3단계 핵심 테스트:
  1순위: Funnel Value-Add Test (파이프라인 단계별 가치 창출 검증)
  2순위: OOS Percentile Tracking (최종 팩터 OOS 백분위 생존율)
  3순위: Strict Jaccard Index (weight>0 팩터 안정성)

보조 지표:
  4순위: IS-OOS Rank Correlation (팩터 순위 유지력)
  5순위: Deflation Ratio (보조, 단독 판단 금지)
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from service.backtest.result_stitcher import WalkForwardResult

logger = logging.getLogger(__name__)


# ── 1순위: Funnel Value-Add Test ─────────────────────────────────────────


def calc_funnel_value_add(walk_forward_result: WalkForwardResult) -> dict[str, Any]:
    """파이프라인 단계별 가치 창출 검증.

    A. EW_All:    전체 유효 팩터 동일가중 (시장/팩터 베타)
    B. EW_Top50:  1차 필터링 후 동일가중 (필터링 실력)
    C. MP_Final:  최종 가중 포트폴리오 (최종 실력)

    판별 기준 (CAGR 기준):
      C > B > A -> 정상 (필터링+최적화 모두 가치 창출)
      B > C > A -> 최적화 과적합 (최적화가 오히려 수익 깎음)
      A > B     -> 1차 필터 과적합 (CAGR 기준 필터링 자체가 과거 우연)
    """
    # OOS 데이터가 없으면 판별 불가
    if len(walk_forward_result.oos_returns) == 0:
        return {
            "pattern": "INSUFFICIENT_DATA",
            "ew_all_cagr": 0.0, "ew_top50_cagr": 0.0, "mp_cagr": 0.0,
            "ew_all_mdd": 0.0, "ew_top50_mdd": 0.0, "mp_mdd": 0.0,
            "ew_all_sharpe": 0.0, "ew_top50_sharpe": 0.0, "mp_sharpe": 0.0,
            "interpretation": "OOS 데이터 부족 - Funnel Value-Add 판별 불가",
        }

    mp_perf = walk_forward_result.calc_performance()
    ew_all_perf = walk_forward_result.calc_ew_all_performance()
    ew_top50_perf = walk_forward_result.calc_ew_top50_performance()

    cagr_a = ew_all_perf["cagr"]
    cagr_b = ew_top50_perf["cagr"]
    cagr_c = mp_perf["cagr"]

    mdd_a = ew_all_perf["mdd"]
    mdd_b = ew_top50_perf["mdd"]
    mdd_c = mp_perf["mdd"]

    # 판별
    if cagr_a > cagr_b:
        pattern = "FILTER_OVERFIT"
        interpretation = (
            f"A(EW_All)={cagr_a:.4%} > B(EW_Top50)={cagr_b:.4%} "
            f"-> 1차 필터 과적합: CAGR 기반 Top-50 선정 자체가 과거 우연. "
            f"하위 150개 팩터 평균보다도 못함"
        )
    elif cagr_b > cagr_c:
        pattern = "OPTIMIZATION_OVERFIT"
        interpretation = (
            f"B(EW_Top50)={cagr_b:.4%} > C(MP)={cagr_c:.4%} > A(EW_All)={cagr_a:.4%} "
            f"-> 최적화 과적합: Top-50 필터링은 유효하나, 가중치 최적화가 IS를 외워서 OOS 수익을 깎음. "
            f"Top-50 동일가중이 더 나은 결과"
        )
    else:
        pattern = "NORMAL"
        interpretation = (
            f"C(MP)={cagr_c:.4%} > B(EW_Top50)={cagr_b:.4%} > A(EW_All)={cagr_a:.4%} "
            f"-> 정상: 필터링과 가중치 최적화 모두 가치 창출"
        )

    return {
        "pattern": pattern,
        "ew_all_cagr": cagr_a,
        "ew_top50_cagr": cagr_b,
        "mp_cagr": cagr_c,
        "ew_all_mdd": mdd_a,
        "ew_top50_mdd": mdd_b,
        "mp_mdd": mdd_c,
        "ew_all_sharpe": ew_all_perf["sharpe"],
        "ew_top50_sharpe": ew_top50_perf["sharpe"],
        "mp_sharpe": mp_perf["sharpe"],
        "interpretation": interpretation,
    }


# ── 2순위: OOS Percentile Tracking ──────────────────────────────────────


def calc_oos_percentile_tracking(walk_forward_result: WalkForwardResult) -> dict[str, Any]:
    """최종 선정 팩터(weight>0)의 OOS 구간 백분위 생존율.

    각 Tier 2 리밸런싱 구간에서:
    1. 전체 유효 팩터의 OOS 실현 수익률 줄세우기
    2. weight>0 팩터들의 평균 백분위 계산

    해석:
      평균 백분위 상위 40% 이내(값 <= 0.40) -> 견고
      평균 백분위 40~60%                      -> 보통
      평균 백분위 60% 이상(값 >= 0.60)          -> 과적합 의심
    """
    rebal_log = walk_forward_result.rebalance_log
    all_fr_history = walk_forward_result.oos_all_factor_returns_history
    active_history = walk_forward_result.active_factors_history

    if not all_fr_history or not active_history:
        return {
            "avg_percentile": np.nan,
            "period_percentiles": [],
            "interpretation": "데이터 부족 - Percentile Tracking 계산 불가",
        }

    # Tier 2 리밸런싱 시점 인덱스
    weight_rebal_indices = [
        i for i, (_, row) in enumerate(rebal_log.iterrows())
        if row.get("is_weight_rebal", False)
    ]

    if not weight_rebal_indices:
        return {
            "avg_percentile": np.nan,
            "period_percentiles": [],
            "interpretation": "Tier 2 리밸런싱 없음 - Percentile Tracking 계산 불가",
        }

    # 방어적 검증: active_factors_history와 weight_rebal_indices 길이 일치 확인
    if len(active_history) != len(weight_rebal_indices):
        logger.warning(
            "active_factors_history(%d) != weight_rebal_indices(%d) - 인덱스 불일치 가능",
            len(active_history), len(weight_rebal_indices),
        )

    period_percentiles = []
    active_idx = 0

    for k, rebal_i in enumerate(weight_rebal_indices):
        if active_idx >= len(active_history):
            break

        active_set = active_history[active_idx]
        active_idx += 1

        if not active_set:
            continue

        # OOS 구간: 현재 Tier 2 ~ 다음 Tier 2 (또는 끝)
        next_rebal_i = weight_rebal_indices[k + 1] if k + 1 < len(weight_rebal_indices) else len(all_fr_history)
        oos_slice = all_fr_history[rebal_i:next_rebal_i]

        if not oos_slice:
            continue

        # 구간 내 팩터별 누적 수익률 (기하 복리)
        cum_returns: dict[str, float] = {}
        for month_dict in oos_slice:
            for f, r in month_dict.items():
                cum_returns[f] = (1 + cum_returns.get(f, 0.0)) * (1 + r) - 1

        if len(cum_returns) < 3:
            continue

        # 전체 팩터 순위 (높은 수익률 = 낮은 백분위 = 좋음, 동률 평균 처리)
        cum_series = pd.Series(cum_returns)
        rank_map = (cum_series.rank(ascending=False, method="average") / len(cum_series)).to_dict()

        # 선정 팩터의 평균 백분위
        active_percentiles = [rank_map[f] for f in active_set if f in rank_map]
        if active_percentiles:
            period_percentiles.append(np.mean(active_percentiles))

    if not period_percentiles:
        return {
            "avg_percentile": np.nan,
            "period_percentiles": [],
            "interpretation": "유효 구간 없음 - Percentile Tracking 계산 불가",
        }

    avg_pct = np.mean(period_percentiles)

    if avg_pct <= 0.40:
        interp = f"평균 백분위 = {avg_pct:.2%} (상위 {avg_pct:.0%}) -> 견고한 팩터 선정"
    elif avg_pct <= 0.60:
        interp = f"평균 백분위 = {avg_pct:.2%} (상위 {avg_pct:.0%}) -> 보통 (랜덤과 차이 미미)"
    else:
        interp = f"평균 백분위 = {avg_pct:.2%} (상위 {avg_pct:.0%}) -> 과적합 의심 (IS 상위 팩터가 OOS에서 추락)"

    return {
        "avg_percentile": avg_pct,
        "period_percentiles": period_percentiles,
        "interpretation": interp,
    }


# ── 3순위: Strict Jaccard Index ──────────────────────────────────────────


def calc_strict_jaccard(active_factors_history: list[set[str]]) -> dict[str, Any]:
    """비중>0 팩터에 대한 엄격한 Jaccard 안정성 검사.

    Top-50이 아닌, 가중치 최적화를 거쳐 실제로 비중이 할당된
    최종 팩터 집합(스타일 수에 따라 5~14개)에만 적용한다.

    해석:
      > 0.5 -> 안정적 (핵심 팩터 유지)
      0.3~0.5 -> 보통
      < 0.3 -> 불안정 (매 리밸런싱마다 완전히 다른 조합 -> 과적합 의심)

    Note: 집합 크기가 10개 내외로 작아 Jaccard가 예민하게 반응.
    """
    if len(active_factors_history) < 2:
        return {
            "avg_jaccard": np.nan,
            "jaccard_values": [],
            "interpretation": "Tier 2 리밸런싱 2회 미만 - Strict Jaccard 계산 불가",
        }

    jaccard_values = []
    for i in range(1, len(active_factors_history)):
        prev_set = active_factors_history[i - 1]
        curr_set = active_factors_history[i]
        if not prev_set and not curr_set:
            continue
        intersection = len(prev_set & curr_set)
        union = len(prev_set | curr_set)
        jaccard = intersection / union if union > 0 else 0.0
        jaccard_values.append(jaccard)

    if not jaccard_values:
        return {
            "avg_jaccard": np.nan,
            "jaccard_values": [],
            "interpretation": "유효한 비교 구간 없음 - Strict Jaccard 계산 불가",
        }

    avg_jaccard = np.mean(jaccard_values)

    if avg_jaccard > 0.5:
        interp = f"Strict Jaccard = {avg_jaccard:.2f} > 0.5 -> 안정적 (핵심 팩터 유지)"
    elif avg_jaccard > 0.3:
        interp = f"Strict Jaccard = {avg_jaccard:.2f} (0.3~0.5) -> 보통"
    else:
        interp = f"Strict Jaccard = {avg_jaccard:.2f} < 0.3 -> 불안정 (과적합 의심: 매번 다른 조합)"

    return {
        "avg_jaccard": avg_jaccard,
        "jaccard_values": jaccard_values,
        "interpretation": interp,
    }


# ── 4순위 (보조): IS-OOS Rank Correlation ────────────────────────────────


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
            "interpretation": "데이터 부족 - Rank Correlation 계산 불가",
        }

    # 각 Tier 2 리밸런싱 구간에 대해 Spearman 계산
    spearman_values = []
    p_values = []

    # 리밸런싱 시점 인덱스 찾기
    rebal_log = walk_forward_result.rebalance_log
    weight_rebal_indices = [i for i, (_, row) in enumerate(rebal_log.iterrows()) if row.get("is_weight_rebal", False)]

    meta_idx = 0
    for k, rebal_i in enumerate(weight_rebal_indices):
        if meta_idx >= len(meta_history):
            break

        meta = meta_history[meta_idx]
        meta_idx += 1

        if meta is None or "factorAbbreviation" not in meta.columns or "cagr" not in meta.columns:
            continue

        is_factors = meta["factorAbbreviation"].tolist()
        is_cagr = meta["cagr"].tolist()

        # OOS 기간: 현재 리밸런싱 ~ 다음 리밸런싱 (또는 끝)
        next_rebal_i = weight_rebal_indices[k + 1] if k + 1 < len(weight_rebal_indices) else len(oos_history)
        oos_slice = oos_history[rebal_i:next_rebal_i]

        if not oos_slice:
            continue

        # OOS 팩터별 누적 수익률 (기하 복리)
        oos_returns_by_factor: dict[str, float] = {}
        for oos_dict in oos_slice:
            for f, r in oos_dict.items():
                oos_returns_by_factor[f] = (1 + oos_returns_by_factor.get(f, 0.0)) * (1 + r) - 1

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
        interp = f"Rank Corr = {avg_spearman:.2f} > 0.3 -> 양의 상관 (팩터 예측력 있음)"
    elif avg_spearman > -0.1:
        interp = f"Rank Corr = {avg_spearman:.2f} ~= 0 -> IS 순위와 OOS 순위 무관"
    else:
        interp = f"Rank Corr = {avg_spearman:.2f} < 0 -> 음의 상관 (과적합 의심)"

    return {
        "avg_spearman": avg_spearman,
        "p_value": avg_p_value,
        "spearman_values": spearman_values,
        "interpretation": interp,
    }


# ── 5순위 (보조): Deflation Ratio ────────────────────────────────────────


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
        result["interpretation"] = "IS CAGR = 0 - 비율 해석 불가"
    elif full_period_cagr < 0:
        result["deflation_ratio"] = np.nan
        result["interpretation"] = f"IS CAGR 음수({full_period_cagr:.4%}), OOS CAGR({oos_cagr:.4%}) - 비율 해석 스킵"
    else:
        ratio = oos_cagr / full_period_cagr
        result["deflation_ratio"] = ratio
        if ratio > 0.6:
            result["interpretation"] = f"Deflation Ratio = {ratio:.2f} > 0.6 -> 양호 (참고용)"
        elif ratio > 0.3:
            result["interpretation"] = f"Deflation Ratio = {ratio:.2f} (0.3~0.6) -> 주의 (참고용)"
        else:
            result["interpretation"] = f"Deflation Ratio = {ratio:.2f} < 0.3 -> 심각 (참고용)"

    return result


# ── 종합 리포트 ──────────────────────────────────────────────────────────


def generate_overfit_report(walk_forward_result: WalkForwardResult, full_period_cagr: float) -> dict[str, Any]:
    """과적합 종합 진단 리포트를 생성한다.

    Args:
        walk_forward_result: Walk-Forward 결과.
        full_period_cagr: 전체 기간 IS simulation 모드 CAGR.

    Returns:
        종합 진단 리포트 dict.
    """
    # 1순위: Funnel Value-Add Test
    funnel = calc_funnel_value_add(walk_forward_result)

    # 2순위: OOS Percentile Tracking
    percentile = calc_oos_percentile_tracking(walk_forward_result)

    # 3순위: Strict Jaccard
    strict_jaccard = calc_strict_jaccard(walk_forward_result.active_factors_history)

    # 4순위 (보조): IS-OOS Rank Correlation
    rank_corr = calc_is_oos_rank_correlation(walk_forward_result)

    # 5순위 (보조): Deflation Ratio
    deflation = calc_deflation_ratio(walk_forward_result, full_period_cagr)

    # OOS 성과
    mp_perf = walk_forward_result.calc_performance()
    ew_perf = walk_forward_result.calc_ew_performance()
    comparison = walk_forward_result.compare_mp_vs_ew_oos()

    n_oos = len(walk_forward_result.oos_returns)

    report = {
        # 1순위: Funnel Value-Add
        "funnel_pattern": funnel["pattern"],
        "funnel_ew_all_cagr": funnel["ew_all_cagr"],
        "funnel_ew_top50_cagr": funnel["ew_top50_cagr"],
        "funnel_mp_cagr": funnel["mp_cagr"],
        "funnel_ew_all_mdd": funnel["ew_all_mdd"],
        "funnel_ew_top50_mdd": funnel["ew_top50_mdd"],
        "funnel_mp_mdd": funnel["mp_mdd"],
        "funnel_interpretation": funnel["interpretation"],
        # 2순위: OOS Percentile Tracking
        "oos_avg_percentile": percentile["avg_percentile"],
        "oos_percentile_interpretation": percentile["interpretation"],
        # 3순위: Strict Jaccard
        "strict_jaccard": strict_jaccard["avg_jaccard"],
        "strict_jaccard_interpretation": strict_jaccard["interpretation"],
        # 4순위 (보조): IS-OOS Rank Correlation
        "is_oos_rank_spearman": rank_corr["avg_spearman"],
        "rank_corr_p_value": rank_corr["p_value"],
        "rank_corr_interpretation": rank_corr["interpretation"],
        # 5순위 (보조): Deflation Ratio
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
            f"Funnel Value-Add Test(단계별 가치 창출)를 가장 먼저 확인할 것."
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

    # ── 1순위: Funnel Value-Add ──
    funnel_table = Table(title="1순위: Funnel Value-Add Test (단계별 가치 창출)", show_header=True)
    funnel_table.add_column("Portfolio", style="bold")
    funnel_table.add_column("CAGR", justify="right")
    funnel_table.add_column("MDD", justify="right")
    funnel_table.add_column("Description")

    funnel_table.add_row(
        "A. EW_All", f"{report['funnel_ew_all_cagr']:.4%}",
        f"{report['funnel_ew_all_mdd']:.4%}",
        "전체 유효 팩터 동일가중",
    )
    funnel_table.add_row(
        "B. EW_Top50", f"{report['funnel_ew_top50_cagr']:.4%}",
        f"{report['funnel_ew_top50_mdd']:.4%}",
        "Top-50 후보군 동일가중",
    )
    funnel_table.add_row(
        "C. MP_Final", f"{report['funnel_mp_cagr']:.4%}",
        f"{report['funnel_mp_mdd']:.4%}",
        "최종 가중 포트폴리오",
    )

    console.print(funnel_table)

    pattern = report["funnel_pattern"]
    pattern_style = {"NORMAL": "green", "OPTIMIZATION_OVERFIT": "red", "FILTER_OVERFIT": "red"}.get(pattern, "yellow")
    console.print(Panel(report["funnel_interpretation"], title=f"판별: {pattern}", style=pattern_style))

    # ── 진단 지표 테이블 ──
    table = Table(title="과적합 진단 지표 (OOS Walk-Forward)", show_header=True)
    table.add_column("Priority", style="bold", width=10)
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_column("Interpretation")

    # 2순위
    pct = report["oos_avg_percentile"]
    table.add_row(
        "2순위", "OOS Percentile",
        f"{pct:.2%}" if not np.isnan(pct) else "N/A",
        report["oos_percentile_interpretation"],
    )

    # 3순위
    sj = report["strict_jaccard"]
    table.add_row(
        "3순위", "Strict Jaccard",
        f"{sj:.4f}" if not np.isnan(sj) else "N/A",
        report["strict_jaccard_interpretation"],
    )

    # 4순위 (보조)
    spearman = report["is_oos_rank_spearman"]
    table.add_row(
        "4순위(보조)", "IS-OOS Rank Corr",
        f"{spearman:.4f}" if not np.isnan(spearman) else "N/A",
        report["rank_corr_interpretation"],
    )

    # 5순위 (보조)
    deflation = report["deflation_ratio"]
    table.add_row(
        "5순위(보조)", "Deflation Ratio",
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
