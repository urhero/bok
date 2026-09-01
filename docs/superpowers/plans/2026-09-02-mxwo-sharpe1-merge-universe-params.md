# mxwo_sharpe1 -> main 머지 + 유니버스별 파라미터 분리 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `mxwo_sharpe1`을 `main` 계열 브랜치에 머지하고, `BENCHMARK` 하나로 MXCN1A/MXWO 파라미터·경로·데이터가 결정되게 하며, MXCN1A 정본 결과가 머지 전후 동일함을 증명한다.

**Architecture:** `config.py`가 `BENCHMARK`(`.env` 우선, 상수 폴백)로 `PARAM`·`PIPELINE_PARAMS`를 조립한다. `service/paths.py`의 `OUTPUT_DIR`는 항상 `output/{BENCHMARK}`. 유니버스 종속 데이터 파일은 `data/{BENCHMARK}_*` 접두어. 코드 충돌은 전부 sharpe1 쪽 채택.

**Tech Stack:** Python 3 / pandas, pytest, git. 파이썬은 `C:\Users\IKM\.virtualenvs\bok-ZkEKkwfv\Scripts\python.exe`. 설계: `docs/superpowers/specs/2026-09-02-mxwo-sharpe1-merge-universe-params-design.md`.

**실행 환경 주의:** 로컬 `.env`에 `BENCHMARK=MXWO`가 살아 있으므로 모든 파이썬 실행은 `BENCHMARK=<X>`를 환경변수로 명시한다 (`load_dotenv`는 기존 환경변수를 덮어쓰지 않음). `.env`는 절대 수정하지 않는다.

---

### Task 1: 머지 + 코드 충돌 해소 (sharpe1 채택)

**Files:** 충돌 12개 (`git merge` 결과 참조)

- [ ] **Step 1: 머지 시작 (커밋 보류)**

```bash
git merge mxwo_sharpe1 --no-commit --no-ff
```
Expected: 12개 CONFLICT 출력, 머지 커밋 미생성.

- [ ] **Step 2: 코드/테스트 충돌 9개는 sharpe1 쪽으로**

```bash
git checkout --theirs -- service/backtest/walk_forward_engine.py service/pipeline/model_portfolio.py service/pipeline/optimization.py service/report/dashboard.py service/report/dashboard_charts.py service/report/dashboard_data.py service/report/report_generator.py tests/test_unit/test_dashboard_data.py README.md research.md
git add service tests README.md research.md
```
(README/research.md도 일단 sharpe1 본문으로 두고 Task 6에서 유니버스 절 추가.)

- [ ] **Step 3: 대시보드 HTML 은 main(MXCN1A) 쪽**

```bash
git checkout --ours -- output/dashboard_2026-07-31.html
git add output/dashboard_2026-07-31.html
```

- [ ] **Step 4: config.py는 Task 2에서 새로 작성** — 여기서는 sharpe1 쪽으로 임시 채택해 인덱스만 정리

```bash
git checkout --theirs -- config.py && git add config.py
git status --short | grep -E '^(UU|AA|DU|UD)' ; echo "remaining conflicts above (should be empty)"
```

- [ ] **Step 5: main이 손으로 이식했던 기능이 sharpe1 코드에 모두 있는지 확인**

```bash
grep -n "_solve_erc_ccd\|ts_mom" service/pipeline/optimization.py | head
grep -n "def build_dashboard\|corr_regime\|cap_effect\|leaderboard" service/report/dashboard.py | head
```
Expected: 모두 존재.

- [ ] **Step 6: 머지 커밋 (config는 다음 Task에서 교체)**

```bash
git commit -m "merge: mxwo_sharpe1 -> main 계열 (코드 충돌은 sharpe1 채택, config/문서/경로는 후속 커밋)"
```

### Task 2: config.py 유니버스 분리

**Files:** Modify `config.py`, Create `tests/test_unit/test_config_universe.py`

- [ ] **Step 1: 실패 테스트**

```python
# tests/test_unit/test_config_universe.py
# -*- coding: utf-8 -*-
"""BENCHMARK 하나로 PARAM/PIPELINE_PARAMS 가 유니버스별로 조립되는지 (2026-09-02)."""
import importlib
import os

import pytest


def _load(monkeypatch, benchmark):
    monkeypatch.setenv("BENCHMARK", benchmark)
    for k in ("UNIVERSE", "SERVER_NAME", "DB_NAME"):
        monkeypatch.delenv(k, raising=False)
    import config
    return importlib.reload(config)


@pytest.mark.parametrize("bm,cost,dedup,win", [("MXCN1A", 20.0, True, None), ("MXWO", 10.0, False, 48)])
def test_universe_params(monkeypatch, bm, cost, dedup, win):
    cfg = _load(monkeypatch, bm)
    assert cfg.PARAM["benchmark"] == bm
    assert cfg.PARAM["universe"] == cfg.UNIVERSES[bm]["universe"]
    assert cfg.PIPELINE_PARAMS["transaction_cost_bps"] == cost
    assert cfg.PIPELINE_PARAMS["use_cluster_dedup"] is dedup
    assert cfg.PIPELINE_PARAMS["is_window_months"] == win
    assert cfg.PIPELINE_PARAMS["style_cap"] == 0.25  # 공통


def test_unknown_benchmark_fails_fast(monkeypatch):
    monkeypatch.setenv("BENCHMARK", "NOPE")
    import config
    with pytest.raises(KeyError):
        importlib.reload(config)
    monkeypatch.setenv("BENCHMARK", "MXCN1A")
    importlib.reload(config)
```

- [ ] **Step 2: 실패 확인**  `BENCHMARK=MXCN1A python -m pytest tests/test_unit/test_config_universe.py -q` → FAIL (`UNIVERSES` 없음).

- [ ] **Step 3: config.py 재작성** — 구조는 스펙 1절. 요지:

```python
BENCHMARK = os.getenv("BENCHMARK") or "MXCN1A"
UNIVERSES = {
    "MXCN1A": {"universe": "clarifi_mxcn1a_afl", "server_name": "10.206.1.19,9433", "db_name": "GLOBAL"},
    "MXWO":   {"universe": "clarifi_mxwo_afl",   "server_name": "10.206.101.14",    "db_name": "kb_global"},
}
_U = UNIVERSES[BENCHMARK]   # KeyError = 오타 즉시 실패
PARAM = {
    "benchmark": BENCHMARK,
    "universe": os.getenv("UNIVERSE") or _U["universe"],
    "server_name": os.getenv("SERVER_NAME") or _U["server_name"],
    "db_name": os.getenv("DB_NAME") or _U["db_name"],
    "user_name": os.getenv("USER_NAME", ""), "user_pwd": os.getenv("USER_PWD", ""),
    "odbc_name": os.getenv("ODBC_NAME", "ODBC Driver 17 for SQL Server"),
}
_COMMON_PARAMS = {...}                       # 스펙 표의 공통 항목 + 기존 주석
_UNIVERSE_PARAMS = {"MXCN1A": {...10개}, "MXWO": {...10개}}
PIPELINE_PARAMS = {**_COMMON_PARAMS, **_UNIVERSE_PARAMS[BENCHMARK]}
```

- [ ] **Step 4: 통과 확인 + 커밋**

```bash
BENCHMARK=MXCN1A python -m pytest tests/test_unit/test_config_universe.py -q
git add config.py tests/test_unit/test_config_universe.py && git commit -m "feat(config): BENCHMARK 하나로 유니버스별 파라미터/DB 조립 (MXCN1A/MXWO 분리)"
```

### Task 3: 경로·데이터 접두어·gitignore·.env.example

**Files:** Modify `service/paths.py:19-20`, `service/pipeline/model_portfolio.py:455,461`, `service/backtest/walk_forward_engine.py:636`, `service/report/dashboard.py:535`, `param_recheck_runner.py:19`, `tests/test_integration/test_pipeline_end_to_end.py:27`, `tests/test_integration/test_pipeline_real_data.py:40`, `tests/test_integration/test_regression.py:22`, `.gitignore`, `.env.example`; Test `tests/test_unit/test_paths.py`

- [ ] **Step 1: 실패 테스트 추가 (test_paths.py 말미)**

```python
def test_output_dir_is_always_per_benchmark(monkeypatch):
    import importlib, config, service.paths as sp
    monkeypatch.setenv("BENCHMARK", "MXCN1A")
    importlib.reload(config); importlib.reload(sp)
    assert sp.OUTPUT_DIR.name == "MXCN1A" and sp.OUTPUT_DIR.parent.name == "output"
```

- [ ] **Step 2: paths.py**

```python
_BENCHMARK = PARAM["benchmark"]
OUTPUT_DIR = PROJECT_ROOT / "output" / _BENCHMARK   # 유니버스별 출력 분리 (2026-09-02: MXCN1A 예외 제거)
```

- [ ] **Step 3: 데이터 파일 접두어** — 세 곳을 `f"{PARAM['benchmark']}_mp_target_gross.csv"`, `f"{PARAM['benchmark']}_mp_multiplier.csv"`, `f"{PARAM['benchmark']}_bm_returns.csv"`로. dashboard.py 535 는 `dd.DATA_DIR / f"{benchmark}_bm_returns.csv"` (benchmark 는 함수 인자로 받거나 `PARAM["benchmark"]`).

- [ ] **Step 4: 기타 경로** — `param_recheck_runner.py`: `from service.paths import OUTPUT_DIR; OUT = OUTPUT_DIR / "experiments" / "param_recheck"`. 통합테스트 3곳: `from service.paths import OUTPUT_DIR`.

- [ ] **Step 5: .gitignore / .env.example**

```
output/*/experiments/
output/*/benchmark_comparison.csv
!data/*_mp_target_gross.csv
!data/*_mp_multiplier.csv
```
`.env.example`: MXCN1A 블록 활성 + MXWO 블록 주석 (BENCHMARK/UNIVERSE/SERVER_NAME/DB_NAME 4줄씩), 계정 3줄.

- [ ] **Step 6: 로컬 데이터 rename + worktree 로 복사 (검증용)**

```bash
cd C:/Users/IKM/bok/data && mv mp_target_gross.csv MXWO_mp_target_gross.csv && mv mp_multiplier.csv MXWO_mp_multiplier.csv
cp C:/Users/IKM/bok/data/{MXWO_mp_target_gross.csv,MXWO_mp_multiplier.csv,MXWO_bm_returns.csv,MXWO_bmwgt.parquet} <worktree>/data/
```

- [ ] **Step 7: 테스트 + 커밋**

```bash
BENCHMARK=MXCN1A python -m pytest tests/test_unit/test_paths.py -q
git add -A service param_recheck_runner.py tests .gitignore .env.example data/MXWO_mp_target_gross.csv data/MXWO_mp_multiplier.csv
git commit -m "feat(paths): output/{BENCHMARK} 통일 + 배포 배수/BM 데이터 파일 유니버스 접두어"
```

### Task 4: MXCN1A 산출물 폴더 이동

- [ ] **Step 1: sharpe1에서 딸려온 루트 구버전 MXCN1A 파일 삭제**

```bash
git rm -q output/dashboard_2026-05-31.html output/dashboard_2026-06-30.html output/factor_returns_pages_sorted_by_cagr.pdf output/quantile_returns_pages_sorted_by_cagr.pdf output/sector_returns_pages_sorted_by_cagr.pdf
```

- [ ] **Step 2: 나머지 루트 추적 파일 전부 이동**

```bash
mkdir -p output/MXCN1A
git ls-files -z output | grep -zv '^output/MXWO/' | xargs -0 -I{} sh -c 'mkdir -p "output/MXCN1A/$(dirname "${1#output/}")" && git mv "$1" "output/MXCN1A/${1#output/}"' _ {}
git ls-files output | grep -v -E '^output/(MXCN1A|MXWO)/' ; echo "^ should be empty"
```

- [ ] **Step 3: 커밋** `git commit -m "chore(output): MXCN1A 산출물 output/MXCN1A/ 로 이동 (유니버스별 폴더 통일)"`

### Task 5: 유닛 테스트 양쪽 통과

- [ ] `BENCHMARK=MXCN1A python -m pytest tests/test_unit/ -q` → 전부 PASS
- [ ] `BENCHMARK=MXWO python -m pytest tests/test_unit/ -q` → 전부 PASS
- [ ] 실패 시 원인이 (a) 경로 상수 캐시면 테스트 수정, (b) 로직이면 멈추고 보고.

### Task 6: 문서

**Files:** `README.md`, `research.md`, `CLAUDE.MD`

- [ ] README 상단에 "유니버스 전환" 절: `.env` `BENCHMARK` 한 줄(주석 토글) / config 폴백 / `output/{BENCHMARK}/` / 데이터 접두어 / 스펙 1절 파라미터 비교표 + 두 유니버스 정본 수치 (MXCN1A: net Sharpe 0.703, MDD -4.9% / MXWO: net 0.739, MDD -4.80).
- [ ] research.md에 같은 표 + "MXCN1A 고유 근거: docs/experiments/mxcn1a_component_ablation_20260805.md" 링크.
- [ ] CLAUDE.MD 검증 명령을 `BENCHMARK=... python main.py ...`, 비교 대상 경로 `output/{BENCHMARK}/`로.
- [ ] 커밋 `docs: 유니버스 전환/파라미터 비교표 (MXCN1A+MXWO 단일 main)`

### Task 7: MXCN1A 검증 게이트

- [ ] `BENCHMARK=MXCN1A python main.py mp test test_data.csv` → `output/MXCN1A/*_test_test_data.csv` 를 `git diff --stat` 로 확인 (변경 0 기대).
- [ ] `BENCHMARK=MXCN1A python main.py backtest 2009-12-31 2026-07-31` (약 16분, background).
- [ ] `BENCHMARK=MXCN1A python main.py mp 2009-12-31 2026-07-31`.
- [ ] 비교 스크립트(scratchpad): `walk_forward_results.csv`(구) vs `walk_forward_results_2026-07-31.csv`(신) 공통 열 `max abs diff`, Sharpe/CAGR/MDD; `meta_data.csv` vs `meta_data_2026-07-31.csv`; `pivoted_total_agg_wgt_2026-07-31.csv` git diff; `total_aggregated_weights_2026-07-31_test.csv` git diff.
- [ ] **차이 있으면 멈추고 보고.** 없으면 신규 dated 파일을 정본으로 커밋하고 구 무날짜 파일 제거.

### Task 8: MXWO 검증 게이트

- [ ] `BENCHMARK=MXWO python main.py backtest 2015-06-30 2026-07-31`, `mp 2015-06-30 2026-07-31`
- [ ] `git status --short output/MXWO` → 변경 없음(byte-identical) 기대. 차이 있으면 멈추고 보고.

### Task 9: MXCN1A 대시보드·별첨 재생성 (7·8 통과 시)

- [ ] `BENCHMARK=MXCN1A python main.py viz`, `BENCHMARK=MXCN1A python main.py mp 2009-12-31 2026-07-31 --report`
- [ ] 생성물 확인 후 커밋 `chore(output): MXCN1A 대시보드/별첨 최신 코드 재생성`

### Task 10: 최종 보고

- [ ] 검증 수치 표, 남은 결정(main 착지, 로컬 `output/experiments` 이동 안내), 브랜치 상태.
