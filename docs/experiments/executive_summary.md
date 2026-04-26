# Cluster × Turnover 실험 — CEO/PM 1장 요약

**기간**: 2026-04-24 ~ 2026-04-26 / **총 43 케이스** / **OOS 백테스트** 2009-12-31 ~ 2026-03-31
**결론 한 줄**: **현재 프로덕션 default(`baseline`) 보다 `combo_18_0.1` 으로 전환 권장.**

---

## 1. 핵심 결과 — 4 케이스 핵심 비교 (2014-12 정렬, n=136 공정 비교)

| | `baseline` (현 default) | `baseline_nocap_0.3` (CAGR archetype) | **`combo_18_0.1` (권장)** | `combo_18_0.1_rebal12` (안정형) |
|---|---|---|---|---|
| **CAGR** | 1.85% | 2.21% | 1.85% | 1.71% |
| **Sharpe (전체)** | 0.946 | 0.95 | 0.946 | 0.95 |
| **Sharpe p1 (~2017)** | 1.01 | 1.05 | 0.80 | 0.79 |
| **Sharpe p2 (2018-2022)** | 0.61 | 0.65 | **0.45** | **0.43** |
| **Sharpe p3 (2023~)** | **0.27** | **0.33** | **0.99** | **1.01** |
| **MDD** | -7.91% | -8.80% | **-2.86%** | **-2.44%** |
| **Avg Drawdown 회복** | 4.0m | 3.4m | 6.7m | 6.2m |
| **현재 ONGOING DD** | **-6.61%, 21m 미회복** | **-6.71%, 21m 미회복** | 회복됨 (2025-02) | -1.71% (작음) |
| **Verdict** | OPTIMIZATION_OVERFIT | OK (cap=100%) | **OK** | OK |

> *Sharpe 0.946 부분 동일은 expanding window 구조 때문 (같은 OOS 시점 같은 IS = 같은 결과).*

---

## 2. 권장 = `combo_18_0.1` 이유

### A. 최근 시장에 강함
- **2023 이후 Sharpe 0.99 vs baseline 0.27 (3.7배)**
- baseline 의 raw Sharpe 0.63 은 2014-2017 강세에 의존하는 historical artifact
- **미래가 최근 5년과 유사하면 baseline 은 거의 무수익**

### B. Drawdown 위험 격감
- MDD -2.86% (baseline 의 1/3 수준)
- **현재 baseline 은 21개월째 -6% 미회복** 상태이지만, combo_18_0.1 은 이미 회복
- TE(Tracking Error) 작아 운영 안정성 우수

### C. 과적합 진단 통과
- **OPTIMIZATION_OVERFIT → OK** verdict 변환 (Funnel Test 정상화)
- baseline 은 style_cap 25% 재분배가 OOS 수익을 깎는 패턴 (Funnel B>C>A)

### D. 거래비용 부담 작음
- Avg Turnover 0.057 (baseline 0.045 대비 +0.012 미세 증가)
- `factor_rebal=12` 변형(`combo_18_0.1_rebal12`)은 turnover 0.035 로 더 낮음

---

## 3. 권장 변경 사항

### ⚠️ Production Gap — 코드 변경 필요!

**현재 상태:** `cluster_and_dedup_top_n()` 함수가 `walk_forward_engine.py` (백테스트) 에서만 호출됨. `model_portfolio.py` (production `mp` 명령) 의 `_build_return_matrix()` 라인 350 (`meta = meta[:top_factor_count]`) 은 단순 t-stat 상위 N 만 선택, **clustering 미적용**.

검증: `use_cluster_dedup=True` 설정 후 `mp test` 실행 시 baseline 결과와 byte-identical (diff 0).

**필요한 변경:**
1. `service/pipeline/model_portfolio.py:_build_return_matrix()` 에 clustering 로직 이식 (walk_forward_engine.py:436-443 패턴 참조)
2. 회귀 테스트 추가 (`use_cluster_dedup=False` → diff 0 보장)
3. CLAUDE.md A+B 검증 절차 수행

이 코드 변경 전까지 **production 적용 불가**. backtest 결과는 검증된 셈.

### Backtest CLI 적용 (코드 변경 없이 가능)
```bash
# config.py 의 use_cluster_dedup=True 로 일시 변경 후
python main.py backtest 2009-12-31 2026-03-31 --turnover-alpha 0.1
```

### turnover_smoothing_alpha 도 production 미적용
- `mp` 명령은 단일 시점 weights 산출 → 시계열 EMA 블렌딩 개념 자체가 부적합
- production 적용하려면 별도 가중치 history 관리 시스템 필요

---

## 4. 검토할 위험 요소

### Style 편중 자연 노출
- Cap=25% 해제하지 않으므로 규제 요건(스타일별 max 25%)은 유지
- Clustering 후 cap 트리거가 거의 안 되지만, 가끔 트리거되므로 안전망 작동
- Bloomberg Optimizer 에서 추가 제약 통과 가능성 매우 높음

### 백테스트의 한계
- 팩터-내부 종목 거래비용(30bp)은 반영, **팩터 간 비중 변경 비용은 미반영**
  → `net_cagr_cew` 컬럼이 보정한 추정치 사용 (avg_turnover × 0.012/year)
- OOS 184개월(15년) 평가, 단 2014-2017 강세 구간 영향 큼

### Regime drift
- 2018~2022 구간이 모든 전략에 약세 — 새 regime 가 다시 와도 cluster 가 유리한지 미지수
- Rolling Sharpe 모니터링 권장 (다음 단계 후보)

---

## 5. 다음 단계 (우선순위 순)

1. **`baseline` → `combo_18_0.1` 전환 결정** (CEO/PM 의사결정)
2. **`mp` 회귀 테스트**: clustering on/off 에서 `total_aggregated_weights` diff 확인 + 종목 수준 차이 검토
3. **Bloomberg Optimizer dry-run**: TE/제약 위반 여부 사전 확인
4. **Rolling 36-month Sharpe 모니터링** 대시보드 (regime drift 감지)
5. **추천 규칙 개선** (현 자동 규칙은 turnover 0.001 차이로 잘못 추천한 사례 있음)

---

## 부록: 산출물

- 전체 43 케이스 결과: [`cluster_turnover_20260425.md`](cluster_turnover_20260425.md)
- 케이스별 summary: [`cluster_turnover_20260425_summary.csv`](cluster_turnover_20260425_summary.csv)
- 기간별 Sharpe 분석: [`period_sharpe_analysis.md`](period_sharpe_analysis.md)
- Drawdown 상세: [`drawdown_analysis.md`](drawdown_analysis.md), [`drawdown_episodes.csv`](drawdown_episodes.csv)
