# 죽은 코드/미사용 코드 정리 — 실행 계획 (4-agent 교차검증 완료)

- 날짜: 2026-06-23 (검증 보강 2026-06-24)
- 범위: 핵심 코드(`service/`, `db/`, `main.py`, `config.py`) + 전체 미사용 코드 스윕 + 기존 고아 테스트 fixture
- 제외: `scripts/`(실험 스크립트 본체), `docs/` 일반 문서 (단 load-bearing 문서는 C단계에서 갱신)
- 탐지: `vulture 2.16`, `ruff 0.15.18` (F401/F811/F841 + RUF059) + 수동 grep
- 검증: 독립 에이전트 3개 tier별 교차검증 → 종합 에이전트 1개 → 적대적 에이전트 3개(완전성/Tier4 red-team/프로세스)

## 목표

프로덕션 경로에서 호출되지 않거나 결과가 버려지는 코드를 제거한다. **산출물(`aggregated_weights_*`, `total_aggregated_weights_*`, `meta_data.csv`)은 변경 전후 동일해야 한다.**

## 핵심 교정 사항 (초기 spec 대비)

1. `loop_index`, `factor_name`, `show`는 지역변수가 아니라 **함수 파라미터** — 시그니처 변경 + **호출부 동반 수정** 필수.
2. `factor_name`은 positional이라 호출부의 대응 positional 인자도 같이 제거해야 shift 버그 방지.
3. batch/split 테스트 안의 **parity 테스트**가 삭제 대상 단일 함수를 호출 → 콕 집어 제거/편집.
4. correlation 제거 시 `all_positive_returns` fixture, 통합테스트 `:120` assert도 동반 제거 (초기 누락분).
5. **keep-list**: `track`(walk_forward:21, @371 사용), `numpy`(model_portfolio:24, @501 np.nan 사용) — 절대 삭제 금지.

---

## TIER 1 — 미사용 import / 파라미터 (순수, behavior 불변)

각 파일 **아래 라인부터 위로** 편집 (라인 drift 방지).

1. `db/factor_query.py:16` — `from typing import Any, Dict` 줄 삭제
2. `main.py:153` — 함수내 `from pathlib import Path` 삭제
3. `service/download/download_validation.py:12` — `import numpy as np` 삭제
4. `service/backtest/walk_forward_engine.py:19` — `import numpy as np` 삭제 (`track`@21 **유지**)
5. `service/backtest/walk_forward_engine.py:80` — `kept_idx` 미사용 언팩 변수 → `_`
6. `service/pipeline/model_portfolio.py:26` — `from rich.progress import track` 삭제 (`numpy`@24 **유지**)
7. `service/report/report_generator.py:9` — `from pathlib import Path` 삭제
8. `service/report/report_generator.py:147,148` — `kept_name`, `kept_style` 미사용 언팩 → `_`
9. **[시그니처]** `service/report/report_generator.py:40` — `factor_name`, `show=False` 제거 → `def plot_factor_returns(data, style_name, name, mode, ax=None, dropped=None):`
   - **동반 필수** `:300` — positional `factor_abbr` 제거 → `plot_factor_returns(data, style_name, full_name, mode, ax=ax, dropped=drop_set)`
10. **[시그니처]** `service/backtest/walk_forward_engine.py:242` — 파라미터 `loop_index: int = 0,` 제거
    - **동반 필수** `:502` — `loop_index=i,` 인자 제거 (루프변수 `i`는 다른 곳에서 사용되므로 유지)

**T1 완료 게이트**: `ruff check service/ db/ main.py config.py` → F401/F811/F841 0건. 추가로 `mp test test_data.csv` 1회 (런타임 import 해소 확인).

## TIER 2 — 죽은 함수 (참조 0)

11. `service/report/report_generator.py:320-337` — `generate_stress_test_section` 삭제 (지역 `import pandas` 포함). `if __name__` 블록은 유지.

## TIER 3 — 테스트 전용 함수 + 테스트/fixture

**Function A: `calculate_factor_stats`**
12. `service/pipeline/factor_analysis.py:34-137` — 함수 삭제 (다음 def `filter_and_label_factors`@140; batch는 단일 미호출)
13. `service/pipeline/model_portfolio.py:33` — import tuple에서 `calculate_factor_stats,` 제거 (batch/filter는 유지)
14. `tests/test_unit/test_calculate_factor_stats.py:21` — import에서 `calculate_factor_stats` 제거
15. `tests/test_unit/test_calculate_factor_stats.py` — 단일 호출 클래스 전체 삭제: `TestCalculateFactorStatsBasic`(26-107), `...Lag`(110-128), `...SortOrder`(131-157), `...TestMode`(160-202), `...SectorReturn`(205-238), `...EdgeCases`(241-340), `...DataIntegrity`(343-376)
16. `tests/test_unit/test_calculate_factor_stats.py` — `TestCalculateFactorStatsBatch` 내 **`test_batch_matches_single_version`(@parametrize 407 ~ 430) 삭제** (parity 테스트, 단일 호출). 나머지 batch 테스트(432-461)와 `_make_multi_factor_frame`(383-401) **유지**
17. `tests/conftest.py` — fixture 삭제: `sample_factor_data`(55), `insufficient_history_data`(91), `small_sector_data`(114)
18. **[docstring]** `service/pipeline/factor_analysis.py:156,252` — `calculate_factor_stats()` 형식 참조를 batch로 갱신

**Function B: `selection_churn`**
19. `service/report/dashboard_data.py:286-296` — 함수 삭제 (split은 비의존; dashboard는 `_split`@dashboard.py:139만 사용; scripts의 `"selection_churn"`은 dict 키 문자열로 무관)
20. `tests/test_unit/test_dashboard_data.py` — `test_selection_churn_counts_entries_and_exits`(274-283), `test_selection_churn_zero_when_set_stable`(286-289) 삭제
21. `tests/test_unit/test_dashboard_data.py:303-304` — `test_selection_churn_split_entries_and_exits`(292-304)는 **유지하되** 마지막 parity assert(303 주석+304)만 제거 (split의 entries/exits 단언은 유지). `_weight_history()` 헬퍼(258-263) 유지

**T3 게이트**: `pytest tests/test_unit/test_calculate_factor_stats.py tests/test_unit/test_dashboard_data.py -v`

## TIER 4 — downside correlation (산출물 중립, red-team 입증)

> red-team 결론: correlation.py는 순수함수(`dtype=` 강제복사, 메모리 비공유 실증), `.loc[order,order]`는 identity reindex(KeyError 불가, 검증게이트 아님), RNG/global state 없음, `neg_corr`는 try 밖 + walk_forward는 그 3개 CSV 미생성. 결과가 weight/selection/meta 어디에도 안 들어감. 동일성은 by-construction 보장, diff는 확인 절차.

22-25 (model_portfolio.py, **한 커밋에 atomic**):
22. `:31` — correlation import 삭제
23. `:133` — `self.correlation_matrix: pd.DataFrame | None = None` 삭제
24. `:165` — 언팩 → `self.return_matrix, self.meta = self._evaluate_universe(`
25. `:463` `negative_corr = ...` 줄 삭제 + `:466` return → `return ret_df, meta` (462의 `ret_df = ret_df[order]`는 유지 — 컬럼순서 보존)
26. `service/backtest/walk_forward_engine.py:31` — correlation import 삭제; `:496-498` — `neg_corr` 블록(3줄) 삭제
27. `config.py:51` — `"min_downside_obs": 20,` 삭제 (소비처는 위 두 곳뿐)
28. `service/pipeline/correlation.py` — 모듈 삭제
29. `tests/test_unit/test_downside_correlation.py` — 파일 삭제
30. `tests/conftest.py` — `sample_return_matrix`(142), `all_positive_returns`(158) fixture + "상관관계 테스트용" 헤더 주석(137-139) 삭제
31. `tests/test_integration/test_pipeline_real_data.py:120` — `correlation_matrix` assert 줄만 삭제 (`test_pipeline_stores_intermediate_results` 나머지 유지)
32. `tests/test_integration/test_pipeline_real_data.py` — 섹션5 헤더(259-261)+`TestRealDataCorrelationMatrix`(263-289) 삭제 (다음 클래스 296)
33. **[docstring]** `service/pipeline/model_portfolio.py:11-15` — correlation 모듈 줄 제거 + 블록 일관 정리

## TIER 5 — 기존 고아 테스트 fixture (4개 tier 무관, 사용처 0 확인)

34. `tests/conftest.py` — 사용처 0 fixture/헬퍼 7개 **이름 기준 surgical 삭제** (유지 fixture와 interleave됨): `empty_time_series`(38), `test_data_csv`(197), `factor_info_csv`(205), `sample_sector_return_df`(217), `sample_raw_df_for_filter`(230), `assert_dataframe_equal_with_tolerance`(261), `assert_weights_valid`(289)
    - **유지**: `sample_time_series`(14건), `single_value_time_series`(2건), `sample_style_returns`(16건)

## 보존 (건드리지 않음)

- `service/pipeline/optimization.py:21` "DO NOT DELETE THIS COMMENT" 마커, `optimization.py` 모듈
- keep-list: `track`(walk_forward:21), `numpy`(model_portfolio:24)
- `result_stitcher.py:36 self._raw_results` (scripts에서 사용 — vulture false positive)

---

## 실행 순서 & 검증 (CLAUDE.md 준수)

### 0. 베이스라인 — **첫 T1 편집 전 필수** (T1이 오케스트레이터 모듈 건드림)
- `mp test test_data.csv` → 산출물을 **`output/` 밖 별도 디렉토리**에 스냅샷 (통합테스트가 output/ 덮어씀)
- `mp 2009-12-31 2026-03-31` → 별도 스냅샷
- **walk-forward 백테스트 2009-12-31~2026-03-31 (before)** → 백그라운드로 즉시 시작 (~41분)

### 1. 커밋 전략 — tier당 1커밋 (총 5), main-ikm, non-FF 머지, no-squash
- T4를 격리해 문제 시 단독 revert 가능

### 2. 적용: T1 → T2 → T3 → T4 → T5, 각 tier 후 게이트 통과

### 3. 변경 후 검증
- `pytest tests/ -m "not real_data" -v` (unit+통합 수집/마커 검증; `--strict-markers` 켜짐)
- 데이터 있으면 `pytest tests/test_integration/test_pipeline_real_data.py -v`
- `mp test` + `mp 2009-12-31 2026-03-31` 재실행 → 스냅샷과 diff (**byte-identical**)
- **walk-forward 백테스트 2009-2026 (after)** → before와 diff (전체 범위 before/after, 사용자 결정)
- 비교 대상: `aggregated_weights_*`, `total_aggregated_weights_*`, `meta_data.csv`. diff 발생 시 즉시 중단.

### 4. 마무리
- 문서 갱신: `README.md`(194,307), `research.md`(correlation/calculate_factor_stats + stale `rank_negative_correlation`), **`docs/VARIABLE_FLOW.md`**(load-bearing)
- `Pipfile [dev-packages]`에 `vulture`, `ruff` 추가 (T1 커밋에 포함)
- `scripts/build_playground.py:88` correlation 표시문자열은 별도 정리 시 갱신 (생성물 `code_playground.html`)
