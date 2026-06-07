# 절대스텝 스무딩(wr=1) CAGR 하락 팩터/스타일 기여도 분석

- 실행일: 2026-06-07
- Git SHA: `e034ef0`
- 백테스트 기간: `2009-12-31` ~ `2026-05-31` (OOS 162개월)
- 비교 대상: **무스무딩**(`turnover_step=1.0, deadband=0.0`, 현 디폴트) vs **절대스텝**(`turnover_step=0.01, deadband=0.003`)
- 공통 파라미터: min_is=36, factor_rebal=6, **weight_rebal=1**(월간 cadence), top=50, ranking=tstat, cluster_dedup(n=18)

## 0. 동기

3-way 백테스트에서 월간 cadence(wr=1) 절대스텝 스무딩이 turnover 를 ~32% 줄이는 대신 CAGR 이 1.737% -> 1.663% 로 하락했다. 어떤 팩터/스타일이 이 하락을 유발하는지(특히 "모멘텀 등 빠른 신호 스타일이 늦게 반영되어 손해" 가설) 규명한다.

## 1. 방법론 (재구성 오차 0)

스무딩은 **배포 비중만** 바꾸고 **팩터 선정/raw target 은 바꾸지 않는다**. 두 config 를 모두 실제 walk-forward 엔진으로 돌려, 각 월의 **실제 배포 가중치**(`result` weights)와 config-무관 **전체 팩터 OOS 수익률**을 캡처했다. 팩터별 월간 기여도 = `deploy_weights(배포, 가용팩터)[f] x oos_return[f]`.

- 검증: 월별 재계산 기여도 합 == 엔진 `oos_return` (최대 오차 `0.0`, 양 config 모두). 재계산 CAGR == 엔진 CAGR 16자리 일치.
- 분해: 각 팩터의 `(w_step - w_nosmooth) x ret` 을 **연속(타이밍 lag)** vs **탈락(청산 지연)** 으로 분류. 두 버킷 합 = 총 산술 차이 (완전 분해).
- 스크립트: `_bt_attribution2.py` (per-month 덤프 `output/attribution2_dump_*.pkl`, 표 `output/attribution2_*.csv/json`).

> 참고: 초기 단일런 재구성(`_bt_attribution.py`, 절대스텝 경로를 step_smooth 로 오프라인 복원)은 ~0.046%p 오차로 두 채널을 과대추정했다. 본 노트 수치는 두 엔진 실런 기반.

## 2. 포트폴리오 성과

| 지표 | 무스무딩 | 절대스텝 | 차이 |
|---|---|---|---|
| CAGR | 1.7366% | 1.6625% | **-0.074%p/년** |
| Sharpe | 0.850 | 0.819 | -0.031 |
| MDD | -2.28% | -2.63% | 악화 |
| 평균 월수익(산술) | 0.14532% | 0.13921% | -0.0061%p |

## 3. 하락의 두 채널 (162개월 누적 산술 기여도)

| 채널 | 기여도 | 해석 |
|------|:---:|------|
| 연속 팩터 타이밍 lag | **-1.35%p** | 비중을 1%p/월로 천천히 이동 -> 목표 도달 지연 (지배적 원인) |
| 탈락 팩터 청산 지연 | +0.36%p | 탈락 팩터를 ~3개월 더 보유 -> 소폭 이득 |
| 순합 | -0.99%p | (연 환산 ~= CAGR 차이 -0.074%p) |

## 4. 스타일별 분해 (%p, 누적 산술)

| 스타일 (팩터수) | 타이밍 lag | 청산 효과 | **순합** |
|------|:---:|:---:|:---:|
| Price Momentum (28) | **-0.55** | +0.06 | **-0.49** |
| Volatility (5) | +0.19 | -0.50 | -0.31 |
| Capital Efficiency (17) | -0.26 | -0.00 | -0.26 |
| Historical Growth (33) | -0.15 | -0.08 | -0.23 |
| Valuation (12) | -0.14 | -0.04 | -0.18 |
| Analyst Expectations (19) | **-0.52** | +0.36 | -0.16 |
| Earnings Quality (38) | +0.08 | +0.57 | **+0.65** |

## 5. 가설 검증: "모멘텀 등 빠른 신호가 늦게 반영돼 손해" -> **확인됨**

타이밍 lag(비중이 목표에 늦게 도달하는 순수 효과) 손실은 **신호가 가장 빠른 두 스타일에 집중**된다: Price Momentum(-0.55%p), Analyst Expectations(-0.52%p). 3위 Capital Efficiency(-0.26%p)의 2배. 1%p/월 제한이 빠르게 변하는 신호(모멘텀/추정변화)의 포착을 지연시켜, 신호가 식은 뒤에야 목표 비중에 도달한다. 느린 신호(Value/Quality)는 타이밍 lag 가 무해.

**뉘앙스:**
- Analyst Expectations 는 타이밍 손실(-0.52%p)이 크나 청산 이득(+0.36%p)이 절반 이상 상쇄 -> 순손실 작음(-0.16%p). Price Momentum 은 청산 이득이 거의 없어(+0.06%p) 순기준 최악(-0.49%p).
- Earnings Quality 는 +0.65%p 개선 — 밀려난 EQ 팩터가 청산 기간에도 수익을 냈고, 느린 신호라 타이밍 lag 무해.

## 6. 개별 팩터 (순기준 상하위)

**최악 decliner:** STO(Momentum) -0.49%p[타이밍 -0.40], CVVolPrc30D(Momentum) -0.44%p[청산 -0.37], ChgSalesMargin(EQ) -0.32%p, EPSNumRevFY1C(추정) -0.31%p[타이밍 -0.19], 6MChgTgtPrcGap(추정) -0.18%p[타이밍 -0.18].

**최상 improver:** Chg1YTurnover(Momentum) +0.47%p[청산 +0.34], SUEC(추정) +0.22%p, EPSEstDispFY1C(추정) +0.21%p, UnexpectedRecChg(EQ) +0.20%p.

## 7. 결론

스무딩은 **빠른 신호 스타일(Price Momentum, Analyst Expectations)에 집중적으로 비용**을 물린다. 이것이 무비용 OOS 에서 무스무딩이 우세한 이유이며 **무스무딩 디폴트 결정을 뒷받침**한다. 향후 실거래 비용 때문에 스무딩이 필요해지면, 성과 저하가 이 두 스타일에 집중된다는 점을 전제로 운용해야 한다 (예: 모멘텀/추정 스타일만 step 완화).
