# Cluster Size Diagnostics — Walk-Forward 백테스트 분석

**기간**: 2012-12-31 ~ 2026-03-31 (54 Tier 2 fires)
**설정**: `n_clusters=18`, `per_cluster_keep=3`, `top_factor_count=50`
**산출**: [`docs/experiments/cluster_size_diagnostics.png`](cluster_size_diagnostics.png)

## 핵심 요약

| 지표 | 평균 | 표준편차 | 최소 | 최대 |
|---|---|---|---|---|
| 전체 universe 팩터 수 | 225.07 | 1.65 | 223.00 | 229.00 |
| Cluster 후 selected 팩터 수 | 41.15 | 3.87 | 33.00 | 49.00 |
| 가장 큰 cluster 크기 | 119.09 | 34.03 | 66.00 | 178.00 |
| singleton cluster 수 (size=1) | 3.89 | 1.80 | 1.00 | 9.00 |
| size ≤ 2 cluster 수 | 8.96 | 2.46 | 4.00 | 13.00 |
| Top1 cluster 비중 (%) | 52.95 | 15.25 | 28.95 | 79.02 |
| Top1~3 cluster 누적 비중 (%) | 79.78 | 8.14 | 57.46 | 88.55 |

## 발견

1. **Top1 cluster 가 평균 53% 의 팩터 차지** (53%, 최대 79%) — 매우 skewed
2. **Top1~3 cluster 누적 약 80%** — 사실상 3개 큰 cluster + 다수 작은 cluster
3. **평균 selected = 41.1개** (이론적 최대 18*3=54 보다 적음)
4. **singleton cluster 평균 3.9개** — top_n=50 채우지 못하는 주요 원인

## 시점별 대표 샘플

| 날짜 | universe | selected | top1 size | top1% | size>=18 | singletons | size 분포 |
|---|---|---|---|---|---|---|---|
| 2012-12 | 228 | 43 | 66 | 28.9% | 4 | 3 | 66 44 21 21 17 16 16 7 4 3 2 2 2 2 2 1 1 1 |
| 2016-03 | 224 | 47 | 118 | 52.7% | 2 | 2 | 118 30 16 12 10 5 5 5 3 3 3 3 3 2 2 2 1 1 |
| 2019-09 | 225 | 44 | 150 | 66.7% | 2 | 1 | 150 29 6 5 5 4 3 3 3 2 2 2 2 2 2 2 2 1 |
| 2022-12 | 224 | 37 | 176 | 78.6% | 1 | 4 | 176 14 4 4 4 2 2 2 2 2 2 2 2 2 1 1 1 1 |
| 2026-03 | 227 | 39 | 100 | 44.1% | 2 | 6 | 100 84 12 4 3 3 3 3 3 2 2 2 1 1 1 1 1 1 |

## 극단 케이스

### 가장 skewed (Top1 비중 최대): **2022-03**
- Top1 = 177 factors (79.0%)
- Selected = 35개
- Sizes: [177, 16, 4, 4, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1]

### 가장 균등 (Top1 비중 최소): **2012-12**
- Top1 = 66 factors (28.9%)
- Selected = 43개
- Sizes: [66, 44, 21, 21, 17, 16, 16, 7, 4, 3, 2, 2, 2, 2, 2, 1, 1, 1]

## 함의

- 18 cluster 가 **균등하게** 나뉘지 않고 큰 cluster 1-3개가 대부분 차지
  → 같은 cluster (대부분 Momentum-like) 안에서 dedup 강하게 작동
- Selected 평균 41개 (50 미만) → top_n=50 cap 거의 안 닿음
  → 실질적 dedup 강도는 'cluster size 분포' 가 좌우
- Singleton 평균 4개 → 4 factor 가 자체 cluster (다른 어떤 factor 와도 약상관)
  → 이들이 자동 통과해서 다양성 보장
- 시기에 따라 분포 변동 — 2018-2020 가장 skewed (한 cluster 가 대부분),
  최근 2025+ 는 약간 분산
