# MXWO 지역 중립 랭킹 (Region-Neutral Ranking) 설계

- 날짜: 2026-07-28
- 브랜치: `mxwo_region_neutral` (mxwo_main 하위)
- 목표: MXWO 팩터 L/S walk-forward Sharpe 0.16 → **0.5+** (비용 반영)

## 배경 / 가설

MXWO 첫 백테스트 진단:

| 신호 | 값 | 해석 |
|---|---|---|
| EW_All | -0.30% | 팩터 스프레드 원재료 자체가 죽어 있음 |
| EW_Top50 | +0.41% | 선정은 가치를 만들지만 절대 수준 낮음 |
| CEW | +0.45% / Sharpe 0.16 | min_coverage_pct 채택 후 정본 |
| IS-OOS Rank Corr | 0.07 | IS 순위가 OOS를 거의 예측 못함 |

**가설:** 현재 5분위 랭킹이 `(날짜, 섹터)` 그룹이라 20+개국 종목이 같은 잣대로
랭킹된다. 회계기준·시장 밸류에이션 레짐 차이로 팩터 롱/숏 바스켓이 국가
베팅("싼 일본", "오르는 미국")으로 오염되고, M_RETURN이 **USD 환산**이라 FX
변동까지 스프레드에 섞인다. MXCN1A(단일 시장)에는 없던 구조적 문제로,
EW_All ≈ 0 현상과 정합적이다.

## 설계

### 1. Country 매핑 (재다운로드 불필요)

- `gvkeyiid → country` 는 정적 속성 (전 이력 복수 국가 종목 **0개** 확인, 2026-07-28).
- DB 집계 쿼리 1회로 `data/{benchmark}_country_map.parquet` 생성.
- 다운로드 파이프라인(`run_download_pipeline`) 말미에 맵 재생성 단계를 추가해
  증분 다운로드 시 신규 종목이 자동 반영되게 한다.

### 2. 지역 분류 (4개, 코드 상수)

2026-06 스냅샷 기준 종목 수:

- **NA**: USA(489) CAN(80) MEX(1)
- **EUR**: GBR DEU FRA CHE SWE NLD ITA ESP IRL DNK ISR FIN NOR BEL LUX AUT PRT JEY (~420)
- **JPN**: JPN(168)
- **APAC**: AUS HKG SGP NZL MAC CHN (~95)

미분류(BMU CYM URY 등 역외 등록지, ~7종목)는 경고 로그 후 랭킹 제외.
국가→지역 dict는 `factor_analysis.py` 상수.

### 3. 랭킹 그룹 변경

- `calculate_factor_stats_batch`의 rank/percentile/quantile 그룹:
  `(ddt, sec)` → `(ddt, region, sec)`.
- config `PIPELINE_PARAMS["ranking_group"]`: `"sector"`(기존) / `"region_sector"`.
  기본값에서 **기존과 byte-identical** (MXCN1A 영향 zero).
- `min_sector_stocks=10`이 새 그룹 단위로 그대로 적용 — 얇은 지역-섹터 그룹은
  자동 분위 제외 (별도 fallback 없음, YAGNI).
- region 컬럼은 `_prepare_metadata`에서 country map 조인으로 부착 (단일 지점).

### 4. 불변 유지

- 섹터 필터(음의 스프레드 제거)는 **글로벌 sec 단위 유지** — 지역-섹터 단위로
  쪼개면 규칙 4배·표본 1/4로 노이즈 증가.
- 라벨링·선정·클러스터·가중·style_cap 전부 불변.
- 랭킹은 횡단면이라 walk-forward IS 학습·full-stats 사전계산 **양쪽에 동일 적용**
  (look-ahead 없음; coverage 필터와 달리 구조 파라미터이므로 양쪽 일치가 원칙).

### 5. A/B 프로토콜

1. `ranking_group="sector"`로 기존 정본과 byte-identical 검증
2. 유닛 테스트 (지역 분리 랭킹 동작 + 기본값 보존)
3. `region_sector`로 mp + walk-forward: 전 기간/최근 3년 CAGR·Sharpe·MDD,
   deflation ratio, IS-OOS rank corr
4. 채택 기준: 전 지표 비열화 + 유의미 개선. 목표 Sharpe 0.5+ 미달 시 이 구조
   위에 접근 B(선정 개선)·C(오버레이) 누적.

## 리스크

- 지역 내 표본 감소로 분위 노이즈 증가 가능 (특히 APAC 얇은 섹터).
- 국가 중립화로 "국가 모멘텀"이 알파였던 부분까지 제거될 수 있음 — A/B 숫자로 판정.

## 결과 (2026-07-28 실험 완료)

**지역 중립 랭킹 자체는 기각, 파생 실험(롤링 IS + dedup off)이 채택됨.**

1. region_sector 전면 전환: full Sharpe 0.16→-0.24 기각. 연도별 diff 로 원인 규명 —
   국가 편중(2021·2024 미국 모멘텀)이 노이즈가 아니라 알파원. 단 최근 3년 원재료
   (EW_Top50)는 0.18→0.56 개선 → "선정이 레짐을 못 따라간다" 가설로 전환.
2. 롤링 IS 60개월: EW_Top50 full 0.13→0.20, 3y 0.18→0.75. 그러나 CEW 가 역전 —
   퍼널 절단 실험으로 **클러스터 dedup(winner_median)이 범인** 특정
   (롤링의 불안정한 rank_score 분포에서 중위값 바닥 오작동).
3. 윈도우 스윕 (dedup off): w36~48 고원 (0.43/0.41), w60~72 (0.24/0.25),
   전부 baseline(0.16) 상회. region 은 전 윈도우에서 sector 열위 재확인.
4. **채택: rolling48 + sector + dedup off** — full +1.26%/Sharpe 0.412/MDD -7.0%,
   최근 3년 +2.53%/1.397/-1.2% (비용 반영). 목표 0.5 근접, 3y 초과 달성.
   config: is_window_months=48, use_cluster_dedup=False, ranking_group=sector.
