# Cluster Dedup x Turnover Smoothing 실험 리포트

- 실행일: 2026-04-24 20:15:23
- Git SHA: `a58713c`
- 백테스트 기간: `2009-12-31` ~ `2026-03-31`
- 공통 파라미터: min_is=36, factor_rebal=6, weight_rebal=3, top=50, ranking=tstat

## 1. 성과 요약 (OOS, Net-of-cost)

| 케이스 | CAGR | Net CAGR | Sharpe | MDD | Calmar | Avg Turnover | dCAGR vs base | dTurnover vs base |
|---|---|---|---|---|---|---|---|---|
| `baseline` | 2.31% | 2.25% | 0.632 | -7.91% | 0.292 | 0.045 | +0.00% | +0.000 |
| `cluster_18` | 1.39% | 1.29% | 0.693 | -2.93% | 0.475 | 0.084 | -0.92% | +0.039 |
| `cluster_12` | 2.08% | 1.96% | 0.866 | -3.41% | 0.610 | 0.098 | -0.22% | +0.053 |
| `cluster_24` | 1.23% | 1.14% | 0.611 | -3.97% | 0.310 | 0.073 | -1.08% | +0.028 |
| `smooth_0.7` | 2.31% | 2.27% | 0.633 | -7.96% | 0.291 | 0.040 | +0.01% | -0.005 |
| `smooth_0.5` | 2.32% | 2.28% | 0.633 | -8.00% | 0.290 | 0.037 | +0.01% | -0.009 |
| `combo_18_0.7` | 1.41% | 1.33% | 0.706 | -2.90% | 0.488 | 0.074 | -0.89% | +0.029 |
| `combo_18_0.5` | 1.43% | 1.35% | 0.714 | -2.88% | 0.497 | 0.068 | -0.88% | +0.023 |

## 2. 과적합 진단

| 케이스 | Verdict | Funnel (A/B/C CAGR) | OOS Pctile (lower=better) | Jaccard (higher=better) | Rank Corr (higher=better) | Deflation |
|---|---|---|---|---|---|---|
| `baseline` | **OPTIMIZATION_OVERFIT** | OPT_OVERFIT (B>C>A) (1.11%/2.55%/2.31%) | 47.21% [OK] | 0.801 | 0.136 | 0.386 |
| `cluster_18` | **OK** | OK (C>B>A) (1.11%/1.38%/1.39%) | 50.04% [OK] | 0.646 | 0.121 | 0.363 |
| `cluster_12` | **OPTIMIZATION_OVERFIT** | OPT_OVERFIT (B>C>A) (1.11%/2.12%/2.08%) | 49.04% [OK] | 0.614 | 0.163 | 0.491 |
| `cluster_24` | **OK** | OK (C>B>A) (1.11%/1.22%/1.23%) | 50.14% [OK] | 0.681 | 0.109 | 0.313 |
| `smooth_0.7` | **OPTIMIZATION_OVERFIT** | OPT_OVERFIT (B>C>A) (1.11%/2.55%/2.31%) | 47.36% [OK] | 0.838 | 0.136 | 0.388 |
| `smooth_0.5` | **OPTIMIZATION_OVERFIT** | OPT_OVERFIT (B>C>A) (1.11%/2.55%/2.32%) | 47.36% [OK] | 0.838 | 0.136 | 0.389 |
| `combo_18_0.7` | **OK** | OK (C>B>A) (1.11%/1.38%/1.41%) | 50.04% [OK] | 0.721 | 0.121 | 0.369 |
| `combo_18_0.5` | **OK** | OK (C>B>A) (1.11%/1.38%/1.43%) | 50.04% [OK] | 0.721 | 0.121 | 0.374 |

> *Deflation Ratio = OOS CAGR / IS CAGR. OOS 기간이 짧으면 단독 판단 금지.*

## 3. 해석

### 3.1 Clustering 효과 (baseline vs cluster_18)
- dCAGR: -0.92%, dSharpe: +0.060, dTurnover: +0.039
- Verdict: baseline=`OPTIMIZATION_OVERFIT`, cluster_18=`OK`

### 3.2 n_clusters 민감도 (cluster_12 / cluster_18 / cluster_24)
- `cluster_12`: CAGR 2.08%, Sharpe 0.866, Turnover 0.098, Verdict `OPTIMIZATION_OVERFIT`
- `cluster_18`: CAGR 1.39%, Sharpe 0.693, Turnover 0.084, Verdict `OK`
- `cluster_24`: CAGR 1.23%, Sharpe 0.611, Turnover 0.073, Verdict `OK`

### 3.3 Turnover Smoothing 단독 효과 (baseline / smooth_0.7 / smooth_0.5)
- `baseline` (alpha=1.0): CAGR 2.31%, Turnover 0.045
- `smooth_0.7` (alpha=0.7): CAGR 2.31%, Turnover 0.040
- `smooth_0.5` (alpha=0.5): CAGR 2.32%, Turnover 0.037

### 3.4 조합 효과 (cluster_18 / combo_18_0.7 / combo_18_0.5)
- `cluster_18` (alpha=1.0): CAGR 1.39%, Sharpe 0.693, Turnover 0.084
- `combo_18_0.7` (alpha=0.7): CAGR 1.41%, Sharpe 0.706, Turnover 0.074
- `combo_18_0.5` (alpha=0.5): CAGR 1.43%, Sharpe 0.714, Turnover 0.068

> *§3 자동 해석은 방향성/수치만 제시. 도메인 해석은 사람이 보강.*

## 4. 추천 조합

- 선정 규칙: `verdict==OK` 중 Sharpe 상위 3개, 그 중 `avg_turnover` 최저
- **최종 추천: `combo_18_0.5`**
  - 근거: CAGR 1.43%, Sharpe 0.714, Avg Turnover 0.068 (baseline 대비 dCAGR -0.88%, dTurnover +0.023)

## 5. 실행 메타

- 워커 수: 2
- 총 소요 시간 (순차 합): 18029.3s

| 케이스 | 상태 | Runtime (s) |
|---|---|---|
| `baseline` | OK | 2229.0 |
| `cluster_18` | OK | 2219.5 |
| `cluster_12` | OK | 2248.7 |
| `cluster_24` | OK | 2260.1 |
| `smooth_0.7` | OK | 2253.0 |
| `smooth_0.5` | OK | 2260.1 |
| `combo_18_0.7` | OK | 2278.4 |
| `combo_18_0.5` | OK | 2280.4 |
