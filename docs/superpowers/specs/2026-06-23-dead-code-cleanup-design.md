# 죽은 코드/미사용 코드 정리 — 설계 문서

- 날짜: 2026-06-23
- 범위: 핵심 코드(`service/`, `db/`, `main.py`, `config.py`) + 전체 미사용 코드 스윕
- 제외: `scripts/`(실험 스크립트), `docs/`(문서) — 별도 작업으로 분리
- 탐지 도구: `vulture`(미사용 함수/변수), `ruff F401/F811/F841`(미사용 import/변수)

## 목표

프로덕션 경로에서 호출되지 않거나 결과가 버려지는 코드를 제거해 가독성과 백테스트 성능을 개선한다. **산출물(`aggregated_weights_*`, `total_aggregated_weights_*`, `meta_data.csv`)은 변경 전후 동일해야 한다.**

## Tier 1 — 무손실 자동수정 (behavior 불변)

미사용 import/지역변수. 동작에 영향 없음.

| 파일 | 항목 |
|------|------|
| `db/factor_query.py:16` | import `Any`, `Dict` |
| `main.py:153` | import `Path` |
| `service/backtest/walk_forward_engine.py:19,242` | import `numpy`, 변수 `loop_index` |
| `service/download/download_validation.py:12` | import `numpy` |
| `service/pipeline/model_portfolio.py:26,33` | import `track`, `calculate_factor_stats` |
| `service/report/report_generator.py:9,40` | import `Path`, 변수 `factor_name`/`show` |

## Tier 2 — 확실한 죽은 함수 (참조 0)

- `generate_stress_test_section` (`service/report/report_generator.py:320`) — 코드/테스트 어디서도 호출 없음. 함수 삭제.

## Tier 3 — 테스트만 살려두는 프로덕션 미사용 함수 (결정: 삭제)

프로덕션 경로는 batch/split 버전만 사용. 단일 버전은 전용 테스트만 호출.

- `calculate_factor_stats` (`service/pipeline/factor_analysis.py:34`) + `tests/test_unit/test_calculate_factor_stats.py`
  - 프로덕션은 `calculate_factor_stats_batch`만 사용 (batch는 단일 버전을 내부 호출하지 않음)
  - 단, batch 테스트(`test_calculate_factor_stats_batch...`)는 보존 — 단일 버전 의존 케이스만 제거
  - conftest의 단일 버전 전용 fixture 정리
- `selection_churn` (`service/report/dashboard_data.py:286`) + 관련 테스트
  - 대시보드는 `selection_churn_split`만 사용
  - `test_dashboard_data.py`의 `selection_churn`(split 아님) 단독 테스트만 제거

## Tier 4 — 죽은 연산: downside correlation (결정: 제거 + 전체 검증)

`calculate_downside_correlation` 결과가 포트폴리오 구성에 전혀 사용되지 않음 (계산 후 버려짐). 백테스트 hot loop의 `neg_corr`는 매 루프 낭비.

**제거 대상:**
- `service/pipeline/correlation.py` — 모듈 삭제
- `config.py:51` — `min_downside_obs` 파라미터 (correlation 전용)
- `service/pipeline/model_portfolio.py`
  - `:13` 주석, `:31` import 제거
  - `:133` `self.correlation_matrix` 초기화 제거
  - `:165` 언패킹 `self.return_matrix, self.correlation_matrix, self.meta` → `self.return_matrix, self.meta`
  - `:463-465` `negative_corr` 계산 제거, `_evaluate_universe` 반환 `(ret_df, negative_corr, meta)` → `(ret_df, meta)`
- `service/backtest/walk_forward_engine.py:31,496-498` — import + `neg_corr` 블록 제거
- `tests/test_unit/test_downside_correlation.py` — 삭제 (286줄)
- `tests/conftest.py:143` — `sample_return_matrix` fixture (삭제 테스트 전용) 제거
- `tests/test_integration/test_pipeline_real_data.py`
  - `:120` `correlation_matrix` assertion 제거
  - `:265-287` `TestRealDataCorrelationMatrix` 클래스 제거

**기각 근거 메모:** 결과가 downstream에 미사용이므로 산출물 동일성은 논리적으로 보장됨. 검증은 이를 확인하는 절차.

## 보존 (건드리지 않음)

- `service/pipeline/optimization.py:21` — "DO NOT DELETE THIS COMMENT" 명시적 보호 마커
- `optimization.py` 모듈 자체 (이름은 레거시지만 `optimize_constrained_weights`로 실사용)
- `scripts/build_playground.py`의 correlation 참조 — 단순 표시 문자열(import 아님), scripts 범위 외. 별도 정리 시 갱신.

## 검증 프로세스 (CLAUDE.md 준수)

### A. 테스트 모드
1. 변경 전 베이스라인: `python main.py mp test test_data.csv` → 산출물 스냅샷
2. 변경 적용
3. 변경 후 재실행 → diff (동일해야 함)
4. `python -m pytest tests/test_unit/ -v` 통과

### B. 실제 데이터
1. 변경 전 베이스라인: `python main.py mp 2009-12-31 2026-03-31` → 산출물 스냅샷
2. 변경 후 재실행 → diff (동일해야 함)
3. (Tier 4가 walk_forward도 건드리므로) `python main.py backtest <범위>` 추가 확인 권장

### C. 마무리
- `README.md`, `research.md`에서 correlation/미사용 함수 언급 갱신
- 비교 대상: `aggregated_weights_*`, `total_aggregated_weights_*`, `meta_data.csv`

## 실행 순서

1. 베이스라인 캡처 (A.1, B.1) — **코드 변경 전 필수**
2. Tier 1 → 2 → 3 → 4 순차 적용 (각 단계 후 pytest)
3. 변경 후 검증 (A.3, B.2)
4. 문서 갱신, 커밋
