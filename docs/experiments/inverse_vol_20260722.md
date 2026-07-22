# Inverse-Vol 팩터 가중 A/B 실험 (2026-07-22)

- 브랜치: `feature/inverse-vol-weighting`
- 러너: `research/inverse_vol_experiment.py` (baseline은 커밋 산출물 재계산, inverse_vol만 1런)
- 판정: **채택 후보** — 전 하위구간 Sharpe/Calmar/MDD 동시 개선, 추가 파라미터 0개

## 아이디어

[6] 가중 결정에서 1/N 동일가중을 **IS 변동성 반비례 가중(1/sigma)** 으로 교체.
스타일 캡 25% 재분배는 동일 유지. 직관: 1/N은 고변동 팩터가 포트폴리오 리스크를
지배하게 방치 — 1/sigma는 팩터별 리스크 기여를 비슷하게 맞춘다 (risk parity의
상관 무시 단순형). 추가 튜닝 파라미터 없음 (vol 하한 1e-6 가드뿐).

구현: `optimization.py` `optimization_mode="inverse_vol"` (기존 캡 재분배 로직 재사용,
equal_weight 경로 무변경 -> off 시 byte-identical). 유닛테스트 3종.

## 결과 (walk-forward OOS, 동일 조건 A/B)

| 케이스 | CAGR | MDD | Sharpe | Calmar |
|--------|------|-----|--------|--------|
| equal_weight (현행) | 1.88% | -10.23% | 0.516 | 0.184 |
| **inverse_vol** | **2.14%** | **-10.14%** | **0.598** | **0.211** |

하위구간 분해 — **전 구간 개선, 역전 없음**:

| span | EW Sharpe/Calmar/MDD | IV Sharpe/Calmar/MDD |
|------|----------------------|----------------------|
| 전반(~2019-09) | 0.417 / 0.223 / -5.3% | **0.570 / 0.373 / -4.5%** |
| 후반(2019-09~) | 0.599 / 0.252 / -10.2% | 0.631 / 0.259 / -10.1% |
| 2023~ | 1.013 / 0.811 / -6.0% | 1.050 / 0.869 / -5.6% |

- 튜닝 파라미터가 없어 과적합 여지가 구조적으로 작음 (그리드 탐색 아님, 단일 명세)
- vol targeting 오버레이(참고 지표)와 독립인 **전략 수익률 자체의 개선**

## 채택 전 확인 사항

1. **MP-level 비용 검증**: inverse_vol은 expanding IS vol 변화로 팩터 가중이 완만히
   드리프트 -> 종목 턴오버가 EW 대비 소폭 증가 가능. factor-level 백테스트는 이를
   반영하지 않음(양쪽 동일 조건이라 비교 공정성은 유지). 결정판은
   `research/mp_level_cost_backtest.py`에 `--optimization-mode` 옵션 추가 후 재실측.
2. 채택 시: config `optimization_mode="inverse_vol"` 전환 + CLAUDE.md 검증 프로세스
   (산출물 변경이 의도된 diff) + README/research.md 갱신.

## 재현

```bash
python research/inverse_vol_experiment.py   # ~16분
```
