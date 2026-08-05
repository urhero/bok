# MXCN1A 컴포넌트 ablation + 3종 채택 (2026-08-04 ~ 08-05)

- 계기: mxwo_sharpe1 스택 전체 이식 A/B가 전 구간 대폭 열위(full Sharpe 0.539 -> 0.117)로
  기각된 후, 스택을 컴포넌트 단위로 분해해 단독 효과를 재측정
- 러너: `bok-sharpe1` worktree의 `ablation_runner.py` (본 저장소 외부, 브랜치 코드 기반)
- 조건: MXCN1A 2009-12~2026-07, 비용 20bp x netting 0.6, baseline = 현행 main
  (expanding IS + winner_median + equal_risk_weight + 분기 리밸 + 히스테리시스 0.5).
  baseline parity 검증: branch 코드 재현 Sharpe 0.5391 = main 실측 일치

## Phase 1: 단독 적용 (15케이스)

| 케이스 | full Sharpe | full MDD | 2023~ Sharpe | 판정 |
|--------|------------|----------|--------------|------|
| TS모멘텀 w3/s0.5 | 0.601 | -10.5% | 0.892 | 채택 후보 (창 3/4/6 고원) |
| ERC 수축 0.5 | 0.592 | -8.5% | 0.999 | 채택 후보 (수축 0.2~0.5 고원) |
| spread 0.05 | 0.548 | -6.1% | 0.855 | 채택 후보 (MDD/Calmar 대폭 개선) |
| 히스테리시스 0.25 | 0.548 | -9.0% | 0.914 | 미채택 (효과 미미) |
| 섹터 숏캡 15% / min_coverage 10% | 0.539 | -10.1% | 0.850 | no-op (MXCN1A에선 안 걸림) |
| dedup off | 0.453 | -11.3% | 0.579 | 기각 |
| 월간 리밸 | 0.392 | -7.6% | 0.747 | 기각 (비용 잠식) |
| 롤링 IS 36/48/60 | 0.030/0.113/0.352 | — | 전부 악화 | 기각 (expanding 우위) |
| baseline | 0.539 | -10.1% | 0.850 | — |

- sharpe1 스택 실패의 주범 = 롤링 IS + dedup off + 월간 리밸. 좋은 컴포넌트가 묻혀 있었음
- 유니버스별 방법론 비대칭 재확인: winner_median dedup은 MXWO에서 열위, MXCN1A에선 필수

## Phase 2: 조합 (가산성 확인)

| 케이스 | full Sharpe | full MDD | Calmar | 2023~ Sharpe |
|--------|------------|----------|--------|--------------|
| **TSM3 + ERC0.5 + spread0.05** | **0.694** | **-5.0%** | **0.464** | **1.050** |
| TSM3 + ERC0.5 | 0.662 | -8.9% | 0.259 | 1.020 |
| TSM3 + spread0.05 | 0.651 | -5.9% | 0.405 | 0.914 |
| (참고) 조합 + s0.3 | 0.706 | -4.9% | 0.481 | 1.083 |

세 컴포넌트가 서로 다른 축(팩터 타이밍 / 가중 배분 / 라벨 품질)이라 겹치지 않고 가산.

## 채택 게이트 3종 (2026-08-05, 전부 통과)

1. **MP-level 실측** (`research/mp_level_cost_backtest.py`, 종목단 실비용):
   net Sharpe 0.584(base) -> **0.724**(combo), MDD -10.0 -> -4.9%, 턴오버 연 3.0 -> 3.1x
   (거의 불변 — TSM 틸트는 선정을 안 건드림), netting 0.53~0.56, parity 1e-16
2. **ERC RC 분포 검증**: Spinu CCD 솔버가 full/60M/36M cov 모두 RC 정확 균등(1.00x),
   비중 붕괴 없음 (MXWO 구 솔버 결함 재현 안 됨). 캡 재분배 후 RC 분산은 캡의 의도된 왜곡
3. **수축 경계**: 0.2/0.3/0.5 = 0.605/0.601/0.592 고원, 절벽 없음.
   조합 s0.3(0.706)이 소폭 우위나 실측 검증은 s0.5만 수행 -> **s0.5 채택** (보수적)

## 이식 시 순서 교정 (branch 대비 의도된 차이)

branch 원구현은 스타일 캡 재분배 **후** TSM 틸트 + 재정규화 -> 캡이 뚫림
(실측: Capital Efficiency 28.3%). MXCN1A의 캡 25%는 프로덕션 규제 요건이므로
main 이식 시 틸트를 **캡 이전(base 가중 단계)** 로 이동 (`optimize_constrained_weights`
내부로 통합 — mp/엔진/mp_level 공용 경로). 교정 후 canonical 백테스트로 성과 재검증
(수치는 walk_forward_results.csv / overfit_diagnostics.csv 참조).

## 최종 채택 (config 기본값)

- `optimization_mode`: `equal_risk_weight` -> **`erc`** (+ `erc_shrinkage: 0.5`)
- `ts_mom_window: 3`, `ts_mom_scale: 0.5` (신규, None/0 = off)
- `spread_threshold_pct`: 0.10 -> **0.05**
