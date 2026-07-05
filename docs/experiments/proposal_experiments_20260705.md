# 제안 검증 실험 (2026-07-05): 선정/필터 로직 개선안 5종 A/B + EW_Top50 진단 복원

> 스크립트: `research/proposal_experiments_20260705.py` (요약: [proposal_experiments_20260705_summary.csv](proposal_experiments_20260705_summary.csv), 원시 곡선: `research/exp_out_20260705/` — git 미추적)
> 실행: walk-forward 2009-12~2026-06 (163 OOS 개월), min_is=36, production config
> (winner_median, hysteresis 0.5, 20bp x 0.6), 변형당 1개 파라미터만 교체.

## 배경

2026-07-05 전략 로직 리뷰에서 나온 개선 후보 5종을 전부 게이트 파라미터로 구현해
(기본값 off = 현행과 byte-identical, mp test/실데이터/pytest 로 회귀 확인) A/B 검증했다.

## 결과 요약

| variant | 내용 | CAGR | MDD | Sharpe | Calmar | Sharpe(최근36M) | 턴오버/월 | 판정 |
|---|---|---|---|---|---|---|---|---|
| **baseline** | 현행 production | +1.88% | -10.23% | 0.516 | 0.184 | 1.134 | 0.0354 | 기준 |
| sector_tstat_1.0 | 섹터 제거에 유의성 게이트 (t<-1.0만 제거) | +0.95% | -7.24% | 0.292 | 0.131 | 0.389 | 0.0278 | **기각** |
| tstat_hl60 | 랭킹 t-stat half-life 60개월 recency 가중 | +1.96% | -10.59% | 0.515 | 0.185 | 1.229 | 0.0415 | **기각** |
| hysteresis_iqr | 히스테리시스 margin = 0.5 x 후보 IQR | +1.88% | -10.23% | 0.517 | 0.184 | 1.134 | 0.0354 | **기각** |
| zero_frac_0.05 | 0수익월 필터를 IS 길이 비례(5%)로 | +1.88% | -10.23% | 0.516 | 0.184 | 1.134 | 0.0354 | **기각** |
| sector_geo | 섹터 스프레드를 기하평균으로 통일 | +1.60% | -6.80% | 0.471 | 0.236 | 0.621 | 0.0337 | **기각** |

## 변형별 해석

- **sector_tstat_1.0**: 명확히 악화. "약간 음수" 섹터를 남기는 관용이 노이즈 완화 이득보다
  스프레드 희석 비용이 훨씬 컸다. 현행 이진 컷(평균<0 제거)이 공격적이지만 옳다.
- **tstat_hl60**: 전 구간 무승부 (Sharpe 0.515 vs 0.516) + 턴오버 +17%. 최근 36개월
  Sharpe 는 +0.10 개선되나, 전 구간 이득 없이 회전비용만 늘어 채택 근거 부족.
  레짐 민감 가중은 나중에 레짐이 실제로 꺾일 때 재평가할 것.
- **hysteresis_iqr**: 후보 rank_score IQR 이 ~1.0 이라 실효 margin 이 거의 불변 -> no-op.
  고정 margin 의 스케일 드리프트 우려는 실증적으로 무시 가능한 수준.
- **zero_frac_0.05**: 모든 지표 완전 동일 — 걸러지는 팩터 집합이 안 바뀜. 고정 10개월 유지.
- **sector_geo**: MDD -6.8%/Calmar 0.236 으로 낙폭은 크게 개선되나 Sharpe -9%,
  최근 36개월 Sharpe 반토막(0.62). 2023~ 약세 구간 회복력이 핵심 리스크였던 이력을
  고려하면 (cluster_turnover_20260425.md 발견 7) 최근 성과를 깎는 트레이드는 부적합.

**결론: 전략 파라미터 변경 없음.** 5개 제안 모두 기각 — 현행 구성(이진 섹터 컷 +
동일가중 t-stat + 절대 margin 0.5 + 고정 zero-filter + 산술평균 스프레드)이 국소 최적임을
재확인. 게이트 파라미터는 코드에 남겨 재실험 가능 (기본 off, 회귀 테스트로 고정).

## 함께 반영: EW_Top50 진단 곡선 복원 (채택)

커밋 `8dfb64e`(최적화 제거) 이후 equal_weight 에선 선정 팩터 전원이 weight>0 이라
`ew_top50_return`(선정 집합) == `ew_return`(선정 집합) 으로 **동일 곡선이 중복 기록**되고
있었다 (163개월 전 구간 diff=0 실측). Funnel Value-Add 의 B 단계가 퇴화해 "랭킹 필터
실력"과 "dedup+히스테리시스 가치"를 분리할 수 없었다.

`_rank_and_select` 가 클러스터 dedup **이전**의 순수 rank_score Top-N 을 별도 반환하도록
복원 (`cew/ew/ew_all` 컬럼은 byte-identical 검증). 복원된 funnel (baseline):

| 단계 | CAGR | Sharpe | MDD | 의미 |
|---|---|---|---|---|
| A. EW_All (전체 유효) | +0.69% | 0.237 | -7.7% | 팩터 베타 |
| B. 랭킹 Top-50 EW (복원) | +1.82% | 0.438 | -12.9% | t-stat 랭킹 가치 (A->B) |
| 선정 EW (`ew_return`) | +2.50% | 0.601 | -12.3% | **dedup+히스테리시스 가치 (B->선정)** |
| C. CEW (style_cap) | +1.88% | 0.516 | -10.2% | 캡: CAGR 내주고 MDD 개선 |

새 발견: winner_median dedup + 히스테리시스가 순수 랭킹 Top-50 대비 **CAGR +0.68%p /
Sharpe +0.16 을 추가 창출** — 기존 중복 곡선에선 보이지 않던 기여.

주의: funnel 패턴 라벨이 CONSTRAINT_DRAG -> NORMAL 로 바뀌는 것은 B 의 재정의
(선정 집합 -> 랭킹 Top-50) 때문이며 전략 변경이 아니다. style_cap 의 CAGR drag 는
진단표의 "OOS 성과 - EW"(선정 EW 2.50% vs CEW 1.88%) 비교에서 계속 보인다.

## netting ratio 재실측 (backtest_cost_multiplier 캘리브레이션)

2026-07-05 재실측 (`research/mp_level_cost_backtest.py`, 163 OOS 개월):

- **netting ratio = 0.532** (2026-07-03 실측 0.574 대비 -0.042 드리프트)
- MP one-way 턴오버 평균 0.263/월 (연 ~3.2x; 구 실측 ~2.8x)
- 월평균 비용: stock-level 10.5bp vs factor-level 전액계상 19.8bp
- parity: |cew - canonical| max 9.9e-17 (엔진 결정 재현 정상)

판정: **multiplier 0.6 유지.** |0.532 - 0.6| = 0.068 < 허용 밴드 0.1 이내이고,
실비용(0.532)보다 약간 높게 계상하는 보수적 방향이라 백테스트가 성과를 과대평가하지 않는다.

운영 체크리스트: **연 1회 (또는 팩터 유니버스/보유 수가 크게 바뀔 때)**
`python research/mp_level_cost_backtest.py` 를 실행해 netting ratio 를 재실측하고,
0.6 에서 ±0.1 이상 벗어나면 `backtest_cost_multiplier` 를 갱신한다.
