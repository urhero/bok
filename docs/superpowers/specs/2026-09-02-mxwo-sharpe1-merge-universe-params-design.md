# mxwo_sharpe1 -> main 머지 + 유니버스별 파라미터 분리 설계

작성 2026-09-02. 사용자 승인 사항 반영.

## 목표

- `mxwo_sharpe1` 브랜치(MXWO 전용, 95 커밋)의 코드·산출물·문서를 전부 `main`에 머지한다.
- 머지 후 `main` 하나에서 스위치 하나로 MXCN1A 또는 MXWO 파이프라인을 실행할 수 있어야 한다.
- 기존 MXCN1A 결과(Sharpe/수익률/비중)는 머지 전후 동일해야 한다. 다르면 사용자 확인 없이 바꾸지 않는다.
- 모든 산출물은 폴더로 유니버스가 구분돼야 한다: `output/MXCN1A/`, `output/MXWO/`.

## 결정 사항 (사용자 답변)

| 항목 | 결정 |
|---|---|
| 유니버스 전환 스위치 | `.env`의 `BENCHMARK`가 우선, 없으면 `config.py` 상수 `BENCHMARK = "MXCN1A"`. `.env`/`.env.example`에는 MXCN1A·MXWO 두 블록을 모두 두고 안 쓰는 쪽을 주석 처리. **로컬 `.env`는 Claude가 수정하지 않는다** (사용자가 직접 관리). |
| MXWO 배포 배수 데이터 파일 | `data/{BENCHMARK}_mp_target_gross.csv`, `data/{BENCHMARK}_mp_multiplier.csv`, `data/{BENCHMARK}_bm_returns.csv`로 유니버스 접두어 분리. MXCN1A는 파일이 없으므로 배수 미적용 (= 기존 결과 유지). |
| `_to_delete/` 폴더 | 그대로 main에 가져온다. |
| README / research.md | sharpe1 본문을 기준으로, 앞부분에 유니버스 전환 절 + MXCN1A/MXWO 파라미터·정본 수치 비교표 추가. MXCN1A 고유 근거(ablation 등)는 표/링크로 보존. |
| 배수 CSV git 추적 전환 | 미결. 설계 승인 시 "설계 수정 필요"로 답했고 수정 요청은 .env 관련뿐이었으므로 **기본값(추적 전환)으로 진행**하되, 사용자가 거부하면 `.gitignore` 예외 2줄만 되돌린다. |

## 1. config.py 구조

```python
BENCHMARK = os.getenv("BENCHMARK") or "MXCN1A"   # .env 우선, 없으면 여기 한 줄

UNIVERSES = {   # 유니버스 -> DB/데이터 식별자 (SERVER_NAME/DB_NAME 은 .env 값이 있으면 그것을 우선)
    "MXCN1A": {"universe": "clarifi_mxcn1a_afl", "server_name": "10.206.1.19,9433", "db_name": "GLOBAL"},
    "MXWO":   {"universe": "clarifi_mxwo_afl",   "server_name": "10.206.101.14",    "db_name": "kb_global"},
}
PARAM = {"benchmark": BENCHMARK, universe/server_name/db_name: .env 값 or UNIVERSES[BENCHMARK],
         user_name/user_pwd/odbc_name: .env}

_COMMON_PARAMS = {...}                         # 두 유니버스 공통
_UNIVERSE_PARAMS = {"MXCN1A": {...}, "MXWO": {...}}   # 유니버스별 오버라이드
PIPELINE_PARAMS = {**_COMMON_PARAMS, **_UNIVERSE_PARAMS[BENCHMARK]}
COUNTRY_TAX_BPS = {...}                        # 국가맵이 있는 유니버스(MXWO)에서만 실효. MXCN1A 는 국가맵 없음 -> 0
TAX_EXCLUSION_THRESHOLD_BP = None
```

`BENCHMARK` 값이 `UNIVERSES`에 없으면 즉시 `KeyError`로 실패시킨다 (오타로 엉뚱한 폴더에 쓰는 것 방지).

### 유니버스별 파라미터 (값은 각 브랜치의 현재 채택값 그대로)

| 키 | MXCN1A (main 현재) | MXWO (sharpe1 현재) |
|---|---|---|
| transaction_cost_bps | 20.0 | 10.0 |
| erc_shrinkage | 0.5 | 0.2 |
| ts_mom_scale | 0.5 | 0.2 |
| use_cluster_dedup | True | False |
| is_window_months | None (expanding) | 48 |
| selection_hysteresis | 0.5 | 0.25 |
| weight_rebal_months | 3 | 1 |
| min_coverage_pct | 0.0 | 0.10 |
| sector_short_cap | None | 0.15 |
| mp_target_gross | None | 0.40 |

공통: style_cap 0.25, backtest_cost_multiplier 0.6, top_factor_count 50, spread_threshold_pct 0.05, min_sector_stocks 10, ranking_group "sector", max_zero_return_months 10, backtest_start/end, optimization_mode "erc", ts_mom_window 3, deploy_step 1.0, bm_short_cap False, factor_ranking_method "tstat", n_clusters 18, per_cluster_keep 3, cluster_method "winner_median", newey_west_lag 3, style_cap_basis "weight", universe_mask 계열.

각 값 옆의 채택 근거 주석(날짜·실험 문서)은 유지한다.

## 2. 경로와 데이터 파일

- `service/paths.py`: `OUTPUT_DIR = PROJECT_ROOT / "output" / PARAM["benchmark"]` (MXCN1A 예외 제거).
- 추적 중인 MXCN1A 산출물: **main 쪽 트리**를 `git mv output/* -> output/MXCN1A/` (`output/MXWO/` 제외). sharpe1 루트 `output/`에 남아 있던 ERC 이전(2026-03 데이터) MXCN1A 구버전 파일은 폐기.
- `.gitignore`: `output/experiments/` -> `output/*/experiments/`, `output/benchmark_comparison.csv` -> `output/*/benchmark_comparison.csv`. `!data/*_mp_target_gross.csv`, `!data/*_mp_multiplier.csv` 예외 추가.
- 코드에서 `DATA_DIR / "mp_target_gross.csv"`, `"mp_multiplier.csv"`, `"MXWO_bm_returns.csv"`를 `f"{benchmark}_..."`로 교체 (model_portfolio, walk_forward_engine, dashboard).
- 로컬 `C:\Users\IKM\bok\data\`의 해당 파일 3개는 `MXWO_` 접두어로 rename (`bmwgt.parquet`는 이미 접두어 있음).
- `param_recheck_runner.py`의 하드코딩 `output/MXWO/experiments/...` -> `OUTPUT_DIR / "experiments" / ...`.
- `tests/test_integration/*`의 `PROJECT_ROOT / "output"` -> `service.paths.OUTPUT_DIR`.
- `research/*.py`의 구 `output/walk_forward_results.csv` 경로는 손대지 않는다 (MXCN1A 시절 일회성 실험 스크립트, 이미 dated 네이밍으로 깨져 있음).
- `.env.example`: MXCN1A 블록(활성) + MXWO 블록(주석) 두 개를 둔다.

## 3. 머지 규칙

- `git merge mxwo_sharpe1` 후 충돌 12개 해소:
  - 코드 9개(`optimization.py`, `model_portfolio.py`, `walk_forward_engine.py`, `dashboard*.py` 3개, `report_generator.py`, `test_dashboard_data.py`, `test_optimize_constrained_weights.py` 자동병합 확인): **sharpe1 쪽 채택**. main의 이식본은 sharpe1 기능의 부분집합이며, sharpe1이 추가한 신규 파라미터는 config에 없으면 전부 no-op이다.
  - `config.py`: 1절 구조로 새로 작성.
  - `output/dashboard_2026-07-31.html`: main 쪽(MXCN1A 최신) 채택 후 `output/MXCN1A/`로 이동.
  - `README.md`, `research.md`: sharpe1 본문 + 유니버스 절/비교표.
- `CLAUDE.MD`: 유니버스 전환법, 검증 명령이 `output/{BENCHMARK}/` 기준임을 명시. sharpe1의 실험 템플릿 줄 유지.
- `docs/experiments/mxcn1a_component_ablation_20260805.md`(main 전용)는 유지.

## 4. 검증 게이트 (통과 전에는 산출물 재생성/커밋 금지)

실행 환경: 이 worktree. `.env`의 `BENCHMARK=MXWO`를 덮어쓰기 위해 명령마다 환경변수 `BENCHMARK=...`를 명시한다 (`load_dotenv`는 기존 환경변수를 덮어쓰지 않음).

1. `python -m pytest tests/test_unit/ -v` (BENCHMARK 양쪽 모두).
2. **MXCN1A**: `mp test test_data.csv`, 실데이터 `backtest`, `mp` 실행 -> main 정본과 비교.
   - `walk_forward_results.csv`(main 무날짜) vs 새 `walk_forward_results_2026-07-31.csv`: OOS 월수익 열 byte/수치 diff, Sharpe/CAGR/MDD.
   - `meta_data.csv`, `pivoted_total_agg_wgt_2026-07-31.csv`, `total_aggregated_weights_*_2026-07-31_test.csv`.
   - **하나라도 다르면**: 차이 수치와 원인 커밋을 보고하고 멈춘다. 사용자가 결정.
3. **MXWO**: 같은 실행 -> `output/MXWO/*_2026-07-31.csv`와 byte-diff (머지가 MXWO를 안 깨뜨렸는지). 배수 CSV rename이 반영돼야 통과한다.
4. 2·3 통과 후에만 MXCN1A 대시보드·별첨 4종을 최신 코드로 재생성해 `output/MXCN1A/`에 커밋. MXWO 산출물은 sharpe1 그대로.

## 5. 착지

- 전 작업은 `claude/mxwo-sharpe1-merge-params-6b8fbf` 브랜치에서 수행. 검증 결과 보고 -> 사용자 승인 -> `main`에 머지.
- `mxwo_sharpe1` 브랜치는 그대로 둔다.
- 사용자 로컬 체크아웃(`C:\Users\IKM\bok`)의 gitignore 대상 `output/experiments/`는 착지 시 `output/MXCN1A/experiments/`로 이동을 안내만 한다 (Claude가 옮기지 않음).

## 비목표

- 파라미터 값 변경, 방법론 변경 없음.
- `research/` 구 실험 스크립트 경로 정비 없음.
- CLI `--benchmark` 스위치 도입 없음.
