# -*- coding: utf-8 -*-
"""팩터 선정 유틸리티 (Sprint 1).

Shrunk t-stat (James-Stein 계열) 랭킹, Hierarchical Clustering 기반
Top-N 중복 제거, Newey-West 보정 t-stat 진단 지표를 제공한다.

모든 함수는 IS 구간 데이터만 입력받아 IS 전용 규칙을 산출한다.
OOS look-ahead 방지는 호출부에서 IS 슬라이스를 정확히 전달하여 보장한다.
"""
from __future__ import annotations

import logging
from typing import Mapping

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compute_tstat(monthly_rets: pd.DataFrame) -> pd.Series:
    """기본 t-stat: mean / (std / sqrt(N))."""
    n = len(monthly_rets)
    if n < 2:
        return pd.Series(0.0, index=monthly_rets.columns)
    std = monthly_rets.std()
    std_safe = std.where(std > 1e-12, np.nan)
    t = monthly_rets.mean() / (std_safe / np.sqrt(n))
    return t.fillna(0.0)


def compute_shrunk_tstat(
    monthly_rets: pd.DataFrame,
    style_map: Mapping[str, str],
) -> pd.Series:
    """James-Stein 계열 shrinkage를 적용한 t-stat.

    각 팩터의 t-stat을 해당 스타일 그룹 평균 쪽으로 lambda 만큼 shrink한다.
    lambda는 그룹 내 분산 대비 그룹 간 분산 비율로 결정 (데이터 주도).

    수식:
        t_i = raw t-stat (팩터 i)
        t_bar_s = 스타일 s 내 t-stat 평균
        var_within_s = 스타일 s 내 t-stat 분산 (sampling noise proxy)
        var_between = 스타일 평균 간 분산 (signal proxy)
        lambda_s = var_within_s / (var_within_s + var_between)
        shrunk_i = lambda_s * t_bar_s + (1 - lambda_s) * t_i

    lambda가 크면(그룹 내 노이즈가 크면) 그룹 평균에 더 끌어당김.
    lambda가 작으면(그룹 간 신호가 강하면) 개별 값 유지.

    Args:
        monthly_rets: IS 구간 팩터별 월간 L-S 수익률 (rows=month, cols=factor).
        style_map: factorAbbreviation -> styleName 매핑.

    Returns:
        팩터별 shrunk t-stat Series.
    """
    raw_t = compute_tstat(monthly_rets)
    factors = raw_t.index.tolist()

    styles = pd.Series({f: style_map.get(f, "Unknown") for f in factors})
    df = pd.DataFrame({"t": raw_t.values, "style": styles.values}, index=factors)

    style_means = df.groupby("style")["t"].mean()
    grand_mean = df["t"].mean()
    var_between = ((style_means - grand_mean) ** 2).mean()

    style_var = df.groupby("style")["t"].var().fillna(0.0)
    # single-member styles: var=0 -> lambda=0 (no shrinkage)

    shrunk = pd.Series(index=factors, dtype=float)
    for style, group in df.groupby("style"):
        t_bar = style_means[style]
        v_within = float(style_var[style])
        if v_within + var_between <= 1e-12:
            lam = 0.0
        else:
            lam = v_within / (v_within + var_between)
            lam = float(np.clip(lam, 0.0, 1.0))
        for f in group.index:
            shrunk[f] = lam * t_bar + (1.0 - lam) * df.loc[f, "t"]

    logger.debug(
        "shrunk_tstat: %d factors, %d styles, var_between=%.4f",
        len(factors), len(style_means), var_between,
    )
    return shrunk.fillna(0.0)


def compute_newey_west_tstat(
    monthly_rets: pd.DataFrame,
    lag: int = 3,
) -> pd.Series:
    """Newey-West 보정 t-stat (진단용, 랭킹 교체 X).

    월간 L-S 수익률의 자기상관을 Bartlett kernel로 보정한 t-stat.
    기본 t-stat 대비 어느 정도 축소되는지 meta_data 상 분포 관찰용.

    수식 (Newey-West 1987):
        gamma_k = (1/N) * sum_{t=k+1..N} (x_t - xbar)(x_{t-k} - xbar)
        w_k = 1 - k/(lag+1)  (Bartlett weight)
        S_nw = gamma_0 + 2 * sum_{k=1..lag} w_k * gamma_k
        se_nw = sqrt(S_nw / N)
        t_nw = xbar / se_nw
    """
    if monthly_rets.empty:
        return pd.Series(dtype=float)

    n = len(monthly_rets)
    if n < lag + 2:
        return compute_tstat(monthly_rets)

    result = {}
    arr = monthly_rets.values
    means = arr.mean(axis=0)
    for j, col in enumerate(monthly_rets.columns):
        x = arr[:, j]
        xbar = means[j]
        dev = x - xbar
        gamma0 = float(np.mean(dev * dev))
        s_nw = gamma0
        for k in range(1, lag + 1):
            w = 1.0 - k / (lag + 1)
            cov_k = float(np.mean(dev[k:] * dev[:-k]))
            s_nw += 2.0 * w * cov_k
        if s_nw <= 0:
            result[col] = 0.0
            continue
        se = np.sqrt(s_nw / n)
        result[col] = xbar / se if se > 0 else 0.0
    return pd.Series(result)


def cluster_and_dedup_top_n(
    monthly_rets: pd.DataFrame,
    rank_score: pd.Series,
    n_clusters: int = 18,
    per_cluster_keep: int = 3,
    top_n: int = 50,
) -> list[str]:
    """Hierarchical Clustering 기반 Top-N 중복 제거.

    IS 구간 팩터 L-S 수익률 상관관계로 1 - |corr| distance를 만들고,
    average linkage hierarchical clustering으로 n_clusters 개 그룹 확정.
    각 클러스터에서 rank_score 상위 per_cluster_keep 개만 통과시킨 뒤,
    전체 rank_score 기준으로 Top-N 최종 선정.

    IS 전용: monthly_rets/rank_score 모두 IS 구간 값이어야 한다.
    호출부 (walk_forward_engine.py) 에서 ret_df_is로 이미 슬라이스됨.

    Args:
        monthly_rets: IS 구간 팩터 월간 수익률 (rows=month, cols=factor).
        rank_score: 팩터별 랭킹 점수 (e.g., shrunk_tstat).
        n_clusters: 클러스터 개수 (기본 18).
        per_cluster_keep: 클러스터당 유지 팩터 수 (기본 3).
        top_n: 최종 반환 팩터 수.

    Returns:
        선정된 팩터 리스트 (길이 <= top_n), rank_score 내림차순.
    """
    factors = list(monthly_rets.columns)
    if len(factors) <= top_n:
        return list(rank_score.reindex(factors).sort_values(ascending=False).index)

    # n_clusters를 팩터 수에 맞춰 bound
    n_clusters_eff = min(n_clusters, len(factors))
    if n_clusters_eff * per_cluster_keep < top_n:
        # per_cluster_keep 을 늘려 Top-N 채울 여지 확보
        per_cluster_keep = max(per_cluster_keep, int(np.ceil(top_n / n_clusters_eff)))

    # 상관행렬 -> distance
    try:
        corr = monthly_rets.corr().fillna(0.0)
    except Exception as e:
        logger.warning("cluster_and_dedup: corr failed (%s), fallback to rank_score sort", e)
        return list(rank_score.sort_values(ascending=False).head(top_n).index)

    dist_mat = 1.0 - corr.abs().values
    np.fill_diagonal(dist_mat, 0.0)
    dist_mat = np.clip(dist_mat, 0.0, 2.0)
    # 대칭성 보정
    dist_mat = (dist_mat + dist_mat.T) / 2.0

    from scipy.cluster.hierarchy import fcluster, linkage
    from scipy.spatial.distance import squareform

    try:
        condensed = squareform(dist_mat, checks=False)
        link = linkage(condensed, method="average")
        labels = fcluster(link, t=n_clusters_eff, criterion="maxclust")
    except Exception as e:
        logger.warning("cluster_and_dedup: linkage failed (%s), fallback to rank_score sort", e)
        return list(rank_score.sort_values(ascending=False).head(top_n).index)

    cluster_df = pd.DataFrame({
        "factor": factors,
        "cluster": labels,
        "score": rank_score.reindex(factors).values,
    })

    # 클러스터별 상위 per_cluster_keep 개만 통과
    survivors = (
        cluster_df.sort_values(["cluster", "score"], ascending=[True, False])
        .groupby("cluster")
        .head(per_cluster_keep)
    )

    # 전체 score 기준 Top-N
    final = survivors.sort_values("score", ascending=False).head(top_n)

    logger.debug(
        "cluster_dedup: %d factors -> %d clusters -> %d survivors -> Top-%d",
        len(factors), n_clusters_eff, len(survivors), len(final),
    )
    return final["factor"].tolist()
