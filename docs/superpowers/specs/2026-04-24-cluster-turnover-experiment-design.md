# Hierarchical Clustering × Turnover Smoothing 백테스트 실험 설계

- **작성일:** 2026-04-24
- **작성자:** inkyu moon (+ Claude)
- **대상 브랜치:** `claude/funny-bell-1d41f7`
- **상위 목표:** 이미 구현되어 있으나 기본 off인 `use_cluster_dedup` 과 `turnover_smoothing_alpha` 의 조합 효과를 Walk-Forward 백테스트로 검증한다.

---

## 1. 배경과 목표

### 1.1 배경

- `service/backtest/factor_selection.py:cluster_and_dedup_top_n()` — Hierarchical Clustering 기반 Top-N 중복 제거 (Sprint 1-B). 현재 `PIPELINE_PARAMS["use_cluster_dedup"] = False` 로 off.
- `WalkForwardEngine.turnover_smoothing_alpha` — EMA 가중치 블렌딩. 현재 기본 `1.0` (스무딩 없음). CLI `--turnover-alpha` 로 조정 가능.
- 두 기능은 독립적으로 테스트된 바 없고, 조합 효과도 확인되지 않았다.

### 1.2 목표 (우선순위 순)

1. **성과 개선 확인 (주)** — `use_cluster_dedup=True` 가 OOS CAGR/Sharpe 를 개선하는지 검증.
2. **Turnover 감소 효과 (부)** — `turnover_smoothing_alpha < 1.0` 이 실제 거래비용을 줄이면서 성과 손실이 허용 범위인지 측정.
3. **과적합 경감 여부** — Clustering 이 `research.md §6.4` 의 과적합 판정(Funnel Value-Add, OOS Percentile) 에 유의미한 개선을 주는지 확인.
4. **운영 후보 조합 도출** — 8 케이스 중 성과·turnover·과적합 지표를 종합해 추천 1~2 케이스 제시.

### 1.3 비목표

- Cluster 파라미터 (`n_clusters`, `per_cluster_keep`) 의 fine-tuning 전수조사.
- 새로운 clustering 알고리즘 도입.
- 운영 배포 결정 (실험 리포트 이후 별도 논의).

---

## 2. 실험 그리드 (8 케이스)

`per_cluster_keep=3` 고정, 한 축씩 변화시켜 효과 해석을 명확하게 분리한다.

| # | 케이스 이름 | `use_cluster_dedup` | `n_clusters` | `per_cluster_keep` | `turnover_alpha` | 설계 의도 |
|---|---|---|---|---|---|---|
| 1 | `baseline` | False | – | – | 1.0 | 현재 프로덕션 기본 (비교 기준) |
| 2 | `cluster_18` | True | 18 | 3 | 1.0 | clustering 단독 (표준) |
| 3 | `cluster_12` | True | 12 | 3 | 1.0 | n_clusters 민감도 (대분류) |
| 4 | `cluster_24` | True | 24 | 3 | 1.0 | n_clusters 민감도 (세분) |
| 5 | `smooth_0.7` | False | – | – | 0.7 | smoothing 단독 (약) |
| 6 | `smooth_0.5` | False | – | – | 0.5 | smoothing 단독 (강) |
| 7 | `combo_18_0.7` | True | 18 | 3 | 0.7 | 둘 다 (표준) |
| 8 | `combo_18_0.5` | True | 18 | 3 | 0.5 | 둘 다 (강) |

### 비교 축

- **Clustering 효과:** #1 vs #2
- **n_clusters 민감도:** #2 vs #3 vs #4 (단조성 체크)
- **Smoothing 효과:** #1 vs #5 vs #6 (α 감소에 따른 trade-off)
- **조합 효과:** #2 vs #7 vs #8 (clustering 위에 smoothing 얹을 때 marginal)
- **최종 후보:** #7, #8 (성과 유지 + turnover 감소 동시 달성 여부)

### 공통 파라미터 (전 케이스 고정)

- Backtest 기간: `2009-12-31 ~ 2026-03-31`
- `min_is_months=36`, `factor_rebal_months=6`, `weight_rebal_months=3`
- `top_factors=50`, `factor_ranking_method="tstat"`

---

## 3. 러너 아키텍처

### 3.1 최소 침습 변경 (프로덕션 경로 무영향)

`service/backtest/walk_forward_engine.py`:

- `WalkForwardEngine.__init__` 시그니처에 `pipeline_params_override: dict | None = None` 인자 1개 추가.
- `run()` 초반 `pp = dict(PIPELINE_PARAMS)` 직후에 `pp.update(self.pipeline_params_override or {})` 한 줄 추가.

CLI 경로 (`main.py backtest ...`) 는 `pipeline_params_override=None` 으로 주입되므로 동작 완전 동일.

### 3.2 러너 스크립트

- 파일: `scripts/run_cluster_turnover_experiment.py`
- 예상 길이: 약 150~200 라인
- 책임:
  1. 그리드 선언 (CASES 리스트)
  2. `ProcessPoolExecutor` 로 케이스 병렬 실행
  3. 케이스별 결과 CSV/JSON 저장
  4. 집계 `summary.csv` + `REPORT.md` 자동 생성
- **실행하지 않는 것:** 데이터 다운로드, 프로덕션 배포, 설정 파일 영구 수정.

### 3.3 주요 동작 흐름 (의사코드)

```python
CASES = [
    {"name": "baseline", "override": {}, "alpha": 1.0},
    {"name": "cluster_18", "override": {
        "use_cluster_dedup": True, "n_clusters": 18, "per_cluster_keep": 3,
    }, "alpha": 1.0},
    # ... 총 8개
]

OUT_ROOT = Path(f"output/experiments/cluster_turnover_{timestamp}")

def run_single_case(case: dict, out_root: Path, common: dict) -> dict:
    # 워커 프로세스 내부
    case_dir = out_root / case["name"]
    case_dir.mkdir(parents=True, exist_ok=True)
    # stdout/stderr 를 run.log 로 리디렉트
    # logging.getLogger() 레벨 WARNING 이상 (rich progress 억제)

    engine = WalkForwardEngine(
        min_is_months=common["min_is_months"],
        factor_rebal_months=common["factor_rebal_months"],
        weight_rebal_months=common["weight_rebal_months"],
        top_factors=common["top_factors"],
        turnover_smoothing_alpha=case["alpha"],
        pipeline_params_override=case["override"],
    )
    result = engine.run(common["start"], common["end"])

    # 결과 파일 저장
    result.to_csv(case_dir / "walk_forward.csv")
    save_overfit_diagnostics(result, case_dir / "overfit_diagnostics.csv")
    save_performance_json(result, case_dir / "performance.json")

    return build_summary_row(case, result)

if __name__ == "__main__":
    args = parse_args()  # --workers (default 4), --sequential
    write_config_json(OUT_ROOT, CASES, args)

    if args.sequential or args.workers == 1:
        rows = [run_single_case(c, OUT_ROOT, COMMON) for c in CASES]
    else:
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(run_single_case, c, OUT_ROOT, COMMON): c for c in CASES}
            rows = []
            for f in as_completed(futures):
                try:
                    rows.append(f.result())
                except Exception as e:
                    rows.append(build_failed_row(futures[f], e))

    summary_df = pd.DataFrame(rows)
    summary_df.to_csv(OUT_ROOT / "summary.csv", index=False)
    render_markdown_report(summary_df, OUT_ROOT / "REPORT.md")
```

### 3.4 병렬화 세부사항

- **모델:** `ProcessPoolExecutor` (GIL 회피, 케이스 독립).
- **워커 기본값:** 4. CLI `--workers N` 로 변경, `--sequential` 로 순차 fallback.
- **메모리:** 각 워커가 parquet 전체 독립 로드 → RAM 사용량 ≈ `N × 단일 케이스 RAM`. 첫 실행 후 단일 케이스 RAM 측정하여 `--workers` 조정.
- **Windows 호환:** `if __name__ == "__main__":` 가드 필수. spawn 방식.
- **로그 격리:** 워커의 stdout/stderr 는 `case_dir/run.log` 로 리디렉트. rich `track()` 진행바는 워커에서 logger level WARNING↑로 억제. 메인 프로세스는 "case X/8 완료" 한 줄씩만 출력.
- **예외 처리:** `future.exception()` 포집. 실패 케이스는 `summary.csv` 에 `status=FAILED` + 에러 메시지 기록, 나머지 케이스는 계속 진행.

### 3.5 트레이드오프

- 케이스 2/3/4/7/8 은 `cluster_and_dedup_top_n()` 호출로 baseline 대비 각 Tier 2 리밸런싱마다 소폭 느림 (예상 +5~15%).
- 병렬화로 wall-clock 은 단축되지만 총 CPU 시간은 동일하거나 조금 증가.

---

## 4. 산출물 구조

```
output/experiments/cluster_turnover_<YYYYMMDD_HHMMSS>/
├── REPORT.md                    # 사람이 읽는 요약 리포트 (한글)
├── summary.csv                  # 케이스별 성과 요약 한 줄씩 (머신 친화)
├── config.json                  # 그리드 + 공통 파라미터 + git SHA + 실행 시각
├── baseline/
│   ├── walk_forward.csv         # OOS 월별 수익률 (CEW/EW/EW_All/EW_Top50 + 누적)
│   ├── overfit_diagnostics.csv  # 과적합 진단 5지표 + verdict
│   ├── performance.json         # CAGR, Sharpe, MDD, Calmar, Avg Turnover, Net-CAGR
│   └── run.log                  # 워커 stdout/stderr
├── cluster_18/                  # ... (동일 구조)
├── cluster_12/
├── cluster_24/
├── smooth_0.7/
├── smooth_0.5/
├── combo_18_0.7/
└── combo_18_0.5/
```

### 4.1 `summary.csv` 컬럼

| 컬럼 | 의미 |
|---|---|
| `case` | 케이스 이름 |
| `use_cluster_dedup`, `n_clusters`, `per_cluster_keep`, `turnover_alpha` | 그리드 값 |
| `status` | `OK` / `FAILED` |
| `cagr_cew`, `sharpe_cew`, `mdd_cew`, `calmar_cew` | OOS CEW 성과 (net-of-cost) |
| `cagr_ew`, `sharpe_ew` | OOS 1/N 벤치마크 |
| `avg_turnover`, `net_cagr_cew` | 거래비용 차감 전후 |
| `funnel_verdict` | `OK (C>B>A)` / `OPT_OVERFIT (B>C>A)` / `FILTER_OVERFIT (A>B)` |
| `funnel_a_cagr`, `funnel_b_cagr`, `funnel_c_cagr` | Funnel Value-Add 서브 값 |
| `oos_pctile_value`, `oos_pctile_flag` | 수치 + `OK`/`WARN` (≥60% → WARN) |
| `strict_jaccard`, `is_oos_rank_corr`, `deflation_ratio` | 나머지 3지표 raw |
| `verdict` | 종합 판정: `OK` / `OPTIMIZATION_OVERFIT` / `FILTER_OVERFIT` / `PERCENTILE_WARN` |
| `runtime_sec` | 실행 시간 (병렬 튜닝용) |

### 4.2 `performance.json` (케이스별)

`summary.csv` 한 행과 동일한 필드. 단일 케이스만 보고 싶을 때 참조용.

### 4.3 `config.json`

```json
{
  "run_id": "cluster_turnover_20260424_141523",
  "git_sha": "ac80cfe",
  "backtest_start": "2009-12-31",
  "backtest_end": "2026-03-31",
  "min_is_months": 36,
  "factor_rebal_months": 6,
  "weight_rebal_months": 3,
  "top_factors": 50,
  "factor_ranking_method": "tstat",
  "workers": 4,
  "cases": [{"name": "baseline", "override": {}, "alpha": 1.0}, ...]
}
```

**재현성 포인트:** `config.json` + 원본 parquet + 러너 스크립트 → 실험 완전 재현 가능.

---

## 5. `REPORT.md` 포맷

```markdown
# Cluster Dedup × Turnover Smoothing 실험 리포트
- 실행일: YYYY-MM-DD
- Git SHA: <sha>
- 백테스트 기간: 2009-12-31 ~ 2026-03-31
- 공통 파라미터: min_is=36, factor_rebal=6, weight_rebal=3, top=50, ranking=tstat

## 1. 성과 요약 (OOS, Net-of-cost)
| 케이스 | CAGR | Sharpe | MDD | Calmar | Avg Turnover | vs Baseline ΔCAGR | ΔTurnover |
|---|---|---|---|---|---|---|---|
| (8행) |

## 2. 과적합 진단
| 케이스 | Verdict | Funnel (A/B/C CAGR) | OOS Pctile ↓ | Jaccard ↑ | Rank Corr ↑ | Deflation |
|---|---|---|---|---|---|---|
| (8행) |

> *Deflation Ratio = OOS CAGR / IS CAGR. OOS 기간이 짧으면 단독 판단 금지.*

## 3. 해석
### 3.1 Clustering 효과 (cases 1 vs 2)
- (자동) CAGR/Sharpe 변화 방향, 과적합 지표 verdict 개수 변화
### 3.2 n_clusters 민감도 (2 vs 3 vs 4)
- (자동) 12/18/24 수치 비교, 단조성 체크
### 3.3 Turnover Smoothing 단독 효과 (1 vs 5 vs 6)
- (자동) α 감소에 따른 CAGR/Turnover trade-off
### 3.4 조합 효과 (2 vs 7 vs 8)
- (자동) marginal ΔCAGR / ΔTurnover

## 4. 추천 조합
- 규칙: `verdict=="OK"` 인 케이스 중 `sharpe_cew` 상위 3개, 그 중 `avg_turnover` 가장 낮은 것
- 최종 추천: `<case_name>` — 근거 수치 2~3개

## 5. 실행 메타
- 워커 수 / 총 소요 / 케이스별 런타임 / 실패 케이스
```

### 5.1 자동 해석의 한계

- §3.1~3.4 의 **방향성·수치**는 자동 채우되, **도메인 해석 문장**(예: "clustering이 diversification을 개선") 은 사람이 덧붙여야 함.
- §4 추천 규칙은 단순 규칙 기반. Pareto front 분석은 이번 스펙 범위 밖.

### 5.2 과적합 진단 verdict 분류 로직

- Funnel Value-Add `A/B/C CAGR` 값에서:
  - `C > B > A` → `funnel_verdict = "OK (C>B>A)"`
  - `B > C > A` → `funnel_verdict = "OPT_OVERFIT (B>C>A)"`
  - `A > B` → `funnel_verdict = "FILTER_OVERFIT (A>B)"`
  - 그 외 패턴 → `"UNCATEGORIZED"` (드물게 발생)
- OOS Percentile: `>= 60%` → `oos_pctile_flag = "WARN"`, else `"OK"`.
- 종합 `verdict`:
  - `funnel_verdict` 가 `OPT_OVERFIT` → `OPTIMIZATION_OVERFIT`
  - `funnel_verdict` 가 `FILTER_OVERFIT` → `FILTER_OVERFIT`
  - `funnel_verdict=OK` 이고 `oos_pctile_flag=OK` → `OK`
  - `funnel_verdict=OK` 이고 `oos_pctile_flag=WARN` → `PERCENTILE_WARN`

---

## 6. 검증 & 마무리

### 6.1 코드 변경 검증 (CLAUDE.md 프로세스)

**A. pytest**

- `tests/test_unit/test_walk_forward_engine_override.py` 신규 추가:
  - `pipeline_params_override=None` 일 때 기존 동작 유지 확인 (baseline 소량 실행)
  - `override={"use_cluster_dedup": True, "n_clusters": 10, "per_cluster_keep": 2}` 주입 시 `pp` 에 반영되는지 (engine 내부 `pp` 를 노출하거나 cluster_and_dedup 호출이 일어나는지 검증)
- `python -m pytest tests/test_unit/ -v` 전체 통과

**B. 회귀 검증 (CLAUDE.md 필수 프로세스)**

1. 변경 전: `python main.py mp test test_data.csv` 출력 저장
2. 변경 후: 동일 명령 재실행
3. `total_aggregated_weights_*_test.csv`, `meta_data.csv` diff 0 확인 (override=None 경로이므로 완전 동일 필요)
4. 변경 전/후 `python main.py backtest test test_data.csv --min-is-months 4` 도 diff 0 확인

### 6.2 실험 실행 체크리스트

1. `python scripts/run_cluster_turnover_experiment.py --workers 4` 실행
2. 첫 케이스 완료 후 단일 케이스 RAM 로그 확인 → 필요 시 `--workers 2` 재실행
3. `output/experiments/cluster_turnover_<ts>/` 전체 생성 확인
4. `REPORT.md` §3 해석 문단에 도메인 해석 보강 (사람 작업)

### 6.3 문서 업데이트

- `docs/experiments/cluster_turnover_YYYYMMDD.md` — 재현성 있는 최종본 커밋 (`output/` 은 gitignore 대상일 수 있음)
- `README.md` — `use_cluster_dedup` / `turnover_smoothing_alpha` 파라미터 표 옆에 "실험 결과는 `docs/experiments/...` 참조" 한 줄 추가
- `research.md` — 실험 결과가 기존 해석을 바꾸는 경우에만 §6 업데이트

---

## 7. 범위 밖

- 병렬화 실패 시 공유 메모리 (mmap parquet) 최적화
- Cluster 파라미터 전수조사 (n_clusters × keep grid)
- Production 적용 의사결정 (CEO/PM 영역)
- 다른 clustering 알고리즘 (DBSCAN, graph-based 등)
- 여러 `factor_ranking_method` 와의 교차 그리드

---

## 8. 리스크 & 완화

| 리스크 | 영향 | 완화 |
|---|---|---|
| 8 케이스 전 실행이 너무 오래 걸림 | 실험 피드백 루프 지연 | 병렬화 (--workers 4) + 첫 실행 RAM 측정 |
| 워커 RAM 폭증 | OOM 실패 | `--workers` 조정, 최악의 경우 `--sequential` |
| `pipeline_params_override` 도입이 기존 경로에 영향 | 프로덕션 회귀 | override=None 경로 회귀 테스트 + diff-0 검증 의무화 |
| 결과가 noisy하여 비교 어려움 | 의사결정 불가 | 기간 분할 (pre/post 2017) 서브 분석을 필요 시 수행 — 다음 반복에서 고려 |
| `cluster_and_dedup_top_n` 이 특정 IS 슬라이스에서 실패 | 일부 케이스 FAILED | 이미 있는 fallback (rank_score sort) 활용 + 로그 확인 |
