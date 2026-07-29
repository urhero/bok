# MXWO Sharpe 사다리 실험 로그 (2026-07-28 ~ 07-30, mxwo_sharpe1 브랜치)

목표: 전체 기간(OOS 97개월) Sharpe 1.0.

## ⚠ 2026-07-30 정정 — "0.564 채택"은 철회됨

당시 ERC 구현(곱셈 반복법)이 음의 상관 팩터에서 붕괴해 **의도치 않은 집중 포트폴리오**
(50개 중 31개 비중 ~0, RC 상위 5개가 53%)를 산출했고, 실측 net 0.564는 그 아티팩트의
성과였다. 대시보드 상관 무리 섹션에서 0% 팩터 무리가 발견돼 규명됨. 재현 시도 전부 실패:

| 가중 방식 (동일 구성: w48+spread005+hyst025+wrebal1+step0.5) | 실측 net Sharpe |
|---|---|
| 붕괴 ERC (아티팩트, 재현 불가) | 0.564 |
| **정식 ERC (Spinu CCD, RC 균등 보장) — 정정 채택** | **0.368** |
| min_var (롱온리 최소분산 — 아티팩트의 의도된 버전) | 0.217 |
| Top15 / Top10 동일가중 (명시적 집중) | 0.146 / 0.049 |
| (참조) 2026-07-28 구 채택본 | 0.368 |

결론: 아티팩트의 +0.2는 어떤 의도된 메커니즘으로도 재현되지 않는 우연 —
**정직한 현재 수준은 실측 net ~0.37** (구 채택본과 동률). 정식 ERC 구성 유지
(최근 3년 우위 + 올바른 분산 기계). 교훈: (1) 가중 알고리즘은 산출 비중의
RC 분포를 반드시 검증할 것, (2) 시각화가 버그를 잡았다 — 비중 0% 무리처럼
"이상해 보이는" 표시를 무시하지 말 것.

이하는 정정 전 기록 (참고용):
~~최종 채택 (2026-07-29): 실측 net Sharpe 0.564 / gross 0.893~~ — 목표 미달로
사용자 승인 하에 목표 조정. 신호 천장(gross)이 ~1.0이라 현 데이터·아키텍처에서 1.0 불가.
gross 선정(selection-cost 0) 변형도 실측 net 0.463으로 기각 (비용 인지 선정 우위 재확인).

채택 config: optimization_mode=erc(shrink 0.7), spread_threshold_pct=0.05,
selection_hysteresis=0.25, weight_rebal_months=1, deploy_step=0.5,
backtest_cost_multiplier=1.1(실측 netting 1.09), 기존: w48/dedup off/sector/tstat/top50.

아래는 1차 factor-level 사다리 기록 (당시 multiplier 0.6 회계 기준 — 이후 실측으로
고회전 구성에서 비용 과소계상이 확인돼 절대값은 상향 편의가 있음. 상대 비교 참고용).

## 사다리 (누적 채택)

| 단계 | 변경 | 전체 Sharpe | 비고 |
|---|---|---|---|
| 출발 (2026-07-28 채택본) | w48 롤링 + dedup off + ERW + hyst 0.5 | 0.374 | |
| 배치 1 | weight_rebal 3->1개월 | 0.437 | factor_rebal 3/랭킹 cagr·shrunk/top30·70 기각 |
| 배치 3 | ERW -> **ERC** (cov 48M, 대각수축 0.5) | 0.559 | cov 12M/24M 기각 (추정 노이즈) |
| 배치 4 | hysteresis 0.5 -> 0.25 | 0.589 | |
| 배치 5 | **spread_threshold 0.10 -> 0.05** | 0.762 | 0.025와 고원 (강건), 0.0은 퇴화 |
| 배치 6 | **erc_shrinkage 0.5 -> 0.7** | **0.809** | CAGR +3.24%, MDD -6.5%, 3y Sharpe 1.368 |

최종 구성: w48 / wrebal 1M / frebal 6M / hyst 0.25 / dedup off / sector 랭킹 / tstat /
Top50 / ERC(cov 48M, diag shrink 0.7) / spread 0.05 / min_coverage 0.10 / 5분위.

## 기각된 시도 (전부 A/B 수치 근거)

- **오버레이류**: vol targeting(성과는 개선되나 수동 배수 운용과 이중이라 사용자 기각),
  전략 모멘텀 on/off·half, vol x 모멘텀 결합
- **구조**: 지역 중립 랭킹(전 윈도우 열위 — 국가 모멘텀이 알파원), 국가 모멘텀 합성
  팩터(주입은 성공했으나 tstat 랭킹에서 Top50 진입 실패 — 선정 경쟁력 없음),
  10분위(버킷 희소화로 붕괴 0.15), 상수상관 수축 타깃(0.44), 윈도우 앙상블(0.36)
- **재점검에서 현값 확인**: w36/w60·w72, frebal 3(MDD -12.9%), hyst 0/0.5,
  top30/40/45/60 (40-50 고원, 60 절벽), shrink 0.3/0.85, cov030(무변화),
  ERW 극한(0.44 — 상관 반영이 +0.37의 핵심)

## 2차: MP-level 비용 실측 (2026-07-29 오후) — 사다리의 비용 착시 발견

factor-level 0.809 후보를 mp_level_cost_backtest 로 실측한 결과 (parity 9.9e-17 검증):

| 구성 (ERC+spread005+shrink07+w48) | factor-level | gross | **실측 net** | 연회전 | netting |
|---|---|---|---|---|---|
| wrebal1 hyst025 step1.0 | 0.809 | **0.990** | 0.448 | 5.6x | 1.80 |
| wrebal1 hyst025 **step0.5** | 0.710 | 0.893 | **0.564** | ~3.3x | 1.09 |
| wrebal1 hyst025 step0.33/0.65/0.8 | - | - | 0.540/0.513/0.396 | - | - |
| wrebal3 hyst025 | 0.639 | 0.808 | 0.473 | 3.6x | 1.21 |
| 어제 채택본 (ERW spread010 wrebal3) | 0.374 | 0.608 | 0.368 | 1.9x | **0.62** |

핵심 발견:
1. **backtest_cost_multiplier 0.6 은 저회전 구성에서만 유효** (구 채택본 실측 0.62 일치).
   고회전 구성(월간 리밸+좁은 밴드)에서는 실비용이 팩터별 전액 계상의 1.8배 —
   factor-level 사다리의 개선분 일부는 비용 과소계상 착시였음.
2. **월간 리밸의 gross 신호는 진짜** (wrebal1 0.99 vs wrebal3 0.81) — 문제는 구현 비용.
3. **deploy_step(부분 조정 배포) 0.5 채택 후보**: 신호 유지 + 트레이드 절반 ->
   실측 net 0.448->0.564 (step 0.33~0.65 고원, 봉우리 0.5).
4. 실측 기준 최종 후보: 기존 채택본 대비 net Sharpe 0.368 -> 0.564 (+53%).
   단 factor-level 지표와 실측 지표의 괴리가 커서, 고회전 구성 평가 시
   반드시 MP-level 실측을 병행해야 함 (multiplier 재보정 필요).

## 한계 및 주의

- 0.809는 ~45회 탐색의 최대치 -> 선택 편향으로 실전 기대치는 이보다 낮음.
- 목표 1.0은 현 데이터(월간, MXWO 대형주, 200+ 팩터)와 현 아키텍처(주식 L/S) 안에서
  도달 근거 없음. 확장 경로: 일간 데이터(주간 리밸), 신규 팩터 테이블, 국가/FX 별도 슬리브.
- 신규 파라미터는 전부 기본값이 기존 동작 보존: n_quantiles=5, erc_shrinkage=0.5,
  erc_shrink_target="diag", erw_vol_window=None, inject_country_momentum=False,
  optimization_mode에 "erc" 추가.
