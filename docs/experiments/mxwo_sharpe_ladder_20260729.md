# MXWO Sharpe 사다리 실험 로그 (2026-07-28 ~ 07-29, mxwo_sharpe1 브랜치)

목표: 전체 기간(OOS 97개월) Sharpe 1.0.
**최종 채택 (2026-07-29): 실측(MP-level) net Sharpe 0.564 / gross 0.893** — 목표 미달로
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
