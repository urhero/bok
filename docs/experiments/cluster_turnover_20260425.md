# Cluster Dedup x Turnover Smoothing 실험 리포트

- 실행일: 2026-04-25 14:09:06
- Git SHA: `d220eea`
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
| `baseline_nocap` | 2.55% | 2.51% | 0.675 | -8.64% | 0.295 | 0.038 | +0.25% | -0.007 |
| `cluster_nocap` | 1.38% | 1.28% | 0.691 | -2.90% | 0.474 | 0.082 | -0.93% | +0.037 |
| `combo_nocap_0.5` | 1.41% | 1.33% | 0.708 | -2.84% | 0.496 | 0.067 | -0.90% | +0.022 |
| `cluster_8` | 1.48% | 1.36% | 0.549 | -6.81% | 0.217 | 0.094 | -0.83% | +0.049 |
| `cluster_15` | 1.68% | 1.57% | 0.760 | -3.14% | 0.534 | 0.090 | -0.63% | +0.045 |
| `cluster_20` | 1.21% | 1.12% | 0.597 | -3.04% | 0.397 | 0.077 | -1.10% | +0.031 |
| `cluster_30` | 1.43% | 1.35% | 0.644 | -3.26% | 0.438 | 0.065 | -0.88% | +0.020 |
| `cluster_40` | 1.12% | 1.04% | 0.456 | -4.88% | 0.230 | 0.068 | -1.18% | +0.023 |
| `cluster_18_keep1` | 1.39% | 1.29% | 0.693 | -2.93% | 0.475 | 0.084 | -0.92% | +0.039 |
| `cluster_18_keep2` | 1.39% | 1.29% | 0.693 | -2.93% | 0.475 | 0.084 | -0.92% | +0.039 |
| `cluster_18_keep5` | 1.78% | 1.69% | 0.814 | -3.45% | 0.517 | 0.078 | -0.52% | +0.033 |
| `combo_18_cap0.5` | 1.41% | 1.33% | 0.708 | -2.84% | 0.496 | 0.067 | -0.90% | +0.022 |
| `combo_18_0.3` | 1.45% | 1.37% | 0.722 | -2.87% | 0.506 | 0.062 | -0.86% | +0.017 |

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
| `baseline_nocap` | **OK** | OK (C>B>A) (1.11%/2.55%/2.55%) | 47.21% [OK] | 0.801 | 0.136 | 0.414 |
| `cluster_nocap` | **OK** | OK (C>B>A) (1.11%/1.38%/1.38%) | 50.04% [OK] | 0.646 | 0.121 | 0.363 |
| `combo_nocap_0.5` | **OK** | OK (C>B>A) (1.11%/1.38%/1.41%) | 50.04% [OK] | 0.721 | 0.121 | 0.372 |
| `cluster_8` | **OPTIMIZATION_OVERFIT** | OPT_OVERFIT (B>C>A) (1.11%/2.00%/1.48%) | 48.95% [OK] | 0.640 | 0.192 | 0.300 |
| `cluster_15` | **OPTIMIZATION_OVERFIT** | OPT_OVERFIT (B>C>A) (1.11%/1.68%/1.68%) | 49.49% [OK] | 0.627 | 0.144 | 0.392 |
| `cluster_20` | **OK** | OK (C>B>A) (1.11%/1.18%/1.21%) | 50.30% [OK] | 0.671 | 0.116 | 0.314 |
| `cluster_30` | **OK** | OK (C>B>A) (1.11%/1.41%/1.43%) | 49.92% [OK] | 0.700 | 0.102 | 0.351 |
| `cluster_40` | **FILTER_OVERFIT** | FILTER_OVERFIT (A>B) (1.11%/1.11%/1.12%) | 50.14% [OK] | 0.691 | 0.111 | 0.253 |
| `cluster_18_keep1` | **OK** | OK (C>B>A) (1.11%/1.38%/1.39%) | 50.04% [OK] | 0.646 | 0.121 | 0.363 |
| `cluster_18_keep2` | **OK** | OK (C>B>A) (1.11%/1.38%/1.39%) | 50.04% [OK] | 0.646 | 0.121 | 0.363 |
| `cluster_18_keep5` | **OPTIMIZATION_OVERFIT** | OPT_OVERFIT (B>C>A) (1.11%/1.80%/1.78%) | 49.16% [OK] | 0.664 | 0.124 | 0.409 |
| `combo_18_cap0.5` | **OK** | OK (C>B>A) (1.11%/1.38%/1.41%) | 50.04% [OK] | 0.721 | 0.121 | 0.372 |
| `combo_18_0.3` | **OK** | OK (C>B>A) (1.11%/1.38%/1.45%) | 50.04% [OK] | 0.721 | 0.121 | 0.379 |

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
- **최종 추천: `combo_18_0.3`**
  - 근거: CAGR 1.45%, Sharpe 0.722, Avg Turnover 0.062 (baseline 대비 dCAGR -0.86%, dTurnover +0.017)

## 4-1. 광역 sweep 관찰 (수동 보강)

### A. n_clusters 곡선 (8 / 12 / 15 / 18 / 20 / 24 / 30 / 40)

| n_clusters | CAGR | Sharpe | Verdict |
|---|---|---|---|
| 8 | 1.48% | 0.549 | OPT_OVERFIT |
| 12 | 2.08% | 0.866 | OPT_OVERFIT |
| 15 | 1.68% | 0.760 | OPT_OVERFIT |
| **18** | 1.39% | 0.693 | **OK** |
| 20 | 1.21% | 0.597 | OK |
| 24 | 1.23% | 0.611 | OK |
| 30 | 1.43% | 0.644 | OK |
| 40 | 1.12% | 0.456 | **FILTER_OVERFIT** |

- **<18**: cluster 가 너무 거칠어 dedup 효과 부족 → style_cap 트리거 → OPT_OVERFIT 반복
- **18~30**: sweet spot (verdict OK), CAGR/Sharpe 는 18 부근 가장 안정
- **40**: 너무 세분화되어 cluster 가 사실상 individual 팩터 = 1차 필터(t-stat) 가치가 사라짐 → FILTER_OVERFIT

n_clusters 18 채택은 합리적. 12 가 Sharpe 0.866 으로 매력적이지만 verdict OPT_OVERFIT 이라 방어적으로 제외.

### B. per_cluster_keep 효과 (n=18 고정)

| keep | CAGR | Sharpe | Verdict | 비고 |
|---|---|---|---|---|
| 1 | 1.39% | 0.693 | OK | **자동 보정 → 효과 keep=3** |
| 2 | 1.39% | 0.693 | OK | **자동 보정 → 효과 keep=3** |
| 3 | 1.39% | 0.693 | OK | 기준 |
| 5 | 1.78% | 0.814 | **OPT_OVERFIT** | 90 후보 → top-50 → cap 트리거 |

> **코드 한계 발견:** `cluster_and_dedup_top_n()` 의 `n_clusters * keep < top_n` 자동 보정 로직 때문에 keep=1/2 가 사실상 keep=3 으로 강제됨. 진짜 강한 dedup(keep<3)을 보려면 top_n 도 함께 줄여야 함 (별도 실험 필요).

### C. 중간 style_cap (combo + cap 0.25 / 0.5 / 1.0)

| 케이스 | cap | CAGR | Sharpe | 비고 |
|---|---|---|---|---|
| `combo_18_0.5` | 0.25 | 1.43% | 0.714 | cap 미세하게 트리거 |
| `combo_18_cap0.5` | 0.5 | 1.41% | 0.708 | **cap 트리거 안 됨** |
| `combo_nocap_0.5` | 1.0 | 1.41% | 0.708 | cap 트리거 안 됨 |

cap=0.5 와 cap=1.0 결과가 완전히 동일 → clustering 후에는 어떤 스타일도 50% 를 넘지 않음. cap=0.25 만 가끔 트리거.

### D. Smoothing α 풀 sweep (combo 18, α = 0.3 / 0.5 / 0.7 / 1.0)

| α | CAGR | Sharpe | Turnover |
|---|---|---|---|
| 1.0 (cluster_18) | 1.39% | 0.693 | 0.084 |
| 0.7 (combo_18_0.7) | 1.41% | 0.706 | 0.074 |
| 0.5 (combo_18_0.5) | 1.43% | 0.714 | 0.068 |
| **0.3 (combo_18_0.3)** | **1.45%** | **0.722** | **0.062** |

**α 단조 패턴 확인** — smoothing 강할수록 CAGR/Sharpe/Turnover 모두 개선. 추가로 α=0.1, 0.2 테스트 가치 있음 (다음 실험).

## 5. 실행 메타

- 워커 수: 2
- 총 소요 시간 (순차 합): 70027.7s

| 케이스 | 상태 | Runtime (s) |
|---|---|---|
| `baseline` | OK | 2232.9 |
| `cluster_18` | OK | 2236.1 |
| `cluster_12` | OK | 2247.1 |
| `cluster_24` | OK | 2257.3 |
| `smooth_0.7` | OK | 2848.4 |
| `smooth_0.5` | OK | 2877.2 |
| `combo_18_0.7` | OK | 3680.2 |
| `combo_18_0.5` | OK | 3710.6 |
| `baseline_nocap` | OK | 3801.4 |
| `cluster_nocap` | OK | 3830.4 |
| `combo_nocap_0.5` | OK | 3622.1 |
| `cluster_8` | OK | 3588.3 |
| `cluster_15` | OK | 3604.1 |
| `cluster_20` | OK | 3638.1 |
| `cluster_30` | OK | 3642.9 |
| `cluster_40` | OK | 3685.0 |
| `cluster_18_keep1` | OK | 3686.2 |
| `cluster_18_keep2` | OK | 3705.1 |
| `cluster_18_keep5` | OK | 3699.4 |
| `combo_18_cap0.5` | OK | 3721.0 |
| `combo_18_0.3` | OK | 3713.8 |
