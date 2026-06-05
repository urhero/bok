# 절대스텝 밴드형 Turnover Smoothing 설계

- **날짜:** 2026-06-05
- **상태:** 설계 승인됨 (구현 계획 대기)
- **선행:** [2026-06-04 EMA 배포 renorm + prune 설계](2026-06-04-mp-turnover-smoothing-fix-design.md) — 이번 설계가 이를 **대체**한다.

---

## 1. 배경 / 문제

직전 작업(EMA α=0.1 + 배포 renorm + 메모리 prune)을 검증하던 중, **스무딩의 본래 목적(실거래 turnover 최소화)을 달성하지 못함**을 발견했다.

- EMA를 "메모리"에 적용하고, 배포는 "현재 선정분만 renorm"하는 구조였다.
- 그 결과 **메모리 비중은 부드러운데(2.63→2.66)** 정작 거래되는 **배포 비중은 출렁였다(2.63→3.48, +0.85%p)**. 탈락 factor(~24%)가 즉시 배포에서 빠지고 그 비중이 renorm으로 살아있는 factor에 재분배되기 때문.
- 즉 EMA가 실거래 turnover를 줄이지 못했다. (turnover 진단 지표 `compute_avg_turnover`가 **메모리 기준**이라 이 문제가 가려져 있었다.)

**근본 트레이드오프:** {gross=1.0, 배포 수 고정, turnover 최소} 중 둘만 가능. 직전 설계는 "gross=1.0 + 배포 수 고정"을 골라 turnover를 희생했다. 본 설계는 **"gross=1.0 + turnover 최소"**를 택하고, 배포 수 증가는 절대스텝이 ~3개월 내 청산으로 bound한다.

---

## 2. 목표 / 비목표

### 목표
- **실거래(배포) turnover 최소화** — 배포 비중에 직접 스무딩 적용.
- 매 회차 각 factor가 목표 쪽으로 **절대 1%p/월**씩만 이동. 미세 변동(<0.3%p)은 거래 안 함(데드밴드).
- 탈락 factor는 즉시 0이 아니라 **점진 청산**(매월 1%p, ~3개월).
- gross = 1.0 유지.
- production·백테스트 동일 로직 공유(`smoothing.py`), 백테스트로 OOS·turnover 재검증.

### 비목표
- 선정 로직(cluster_dedup), style_cap, 5분위 분석 변경 없음.
- EMA(비례) 방식 유지 안 함 — **완전 대체**.
- 배포 factor 수를 고정 상한으로 강제하지 않음(절대스텝이 자연히 ~3개월 청산으로 bound).

---

## 3. 알고리즘

### 3.1 핵심: `step_smooth`
배포 가중치를 **단일 벡터**로 직접 산출한다(메모리/배포 구분 제거).

```
입력:
  target   : 이번 회차 목표 가중치 {factor: w}, 합 1.0 (optimizer 산출 = 현재 선정분)
  prev     : 직전 회차 배포 가중치 {factor: w}, 합 1.0 (없으면 None)
  step     : 월 최대 이동폭 (기본 0.01 = 1%p)
  deadband : 데드밴드 (기본 0.003 = 0.3%p)
  months   : 직전 배포 이후 경과 월수 (cadence A)

max_step = step * months            # production: 1*0.01=0.01, 백테스트(3개월): 0.03

if prev is None: return dict(target) # 첫 회차: 목표 그대로

current = set(target)                # 현재 선정 factor
union   = set(target) | set(prev)

# --- 1) factor별 tentative ---
held = {}        # 데드밴드 고정
movers = {}      # 이동 (tentative 값)
exits_final = {} # 탈락(목표=0) — 최종값 고정, renorm 제외
for f in union:
    t, p = target.get(f, 0.0), prev.get(f, 0.0)
    gap = t - p
    if (f in current) and abs(gap) < deadband:
        held[f] = p                              # 고정 (거래 0)
    elif f not in current:                       # 탈락 (목표=0): 무조건 0쪽 이동
        new_w = p - min(max_step, p)             # p - step (또는 0)
        if new_w > 1e-12: exits_final[f] = new_w # 0 도달하면 drop
    else:                                        # 유지/신규 중 |gap|>=deadband
        delta = max(-max_step, min(max_step, gap)) # 목표쪽 이동, max_step 캡
        movers[f] = p + delta

# --- 2) 정규화 (direction-safe): 탈락분은 최종 고정, 나머지로 잔여 채움 ---
exit_sum = sum(exits_final.values())
residual = 1.0 - exit_sum                        # held+movers 가 채워야 할 합
base_sum = sum(held.values()) + sum(movers.values())
scale = residual / base_sum if base_sum > 0 else 0.0
new = dict(exits_final)
for f, w in {**held, **movers}.items():
    new[f] = w * scale                           # held 도 미세 흡수 (보통 <0.05%p)
return new                                        # 합 1.0
```

**규칙 요약:**
- **유지 & |gap| < 0.3%p** → 직전 유지(거래 0). 단 2)의 미세 흡수 renorm으로 아주 작게 조정될 수 있음(정상월 <0.05%p).
- **유지/신규 & |gap| ≥ 0.3%p** → 목표 쪽 min(max_step, |gap|) 이동.
- **탈락(목표=0)** → 데드밴드 무시, max_step만큼 0쪽 이동(0 되면 제거). **renorm에서 제외해 절대 증가하지 않음**(회원님 요구: 탈락은 무조건 감소).
- **합 1.0**: 탈락분 고정 후, 나머지(held+movers)를 잔여(1−탈락분)에 맞춰 비례 정규화 → "1% 내외로 합계 100%"(정규화 B), 방향 역전 없음.

### 3.2 cadence A
스텝 기준은 **1%p/월**. `months` = 직전 배포 이후 경과 월수.
- production `mp`: 매월 → months=1 → max_step=1%p.
- 백테스트: 가중치 리밸런스 3개월마다 → months=3 → max_step=3%p (= 1%p/월 캘린더 일치).

### 3.3 제거
- `turnover_smoothing_alpha`(EMA) — 제거.
- `turnover_min_weight`(prune) 및 `update_smoothing_memory`/`deploy_weights`의 prune 로직 — 제거. 절대스텝에선 탈락 factor가 ~3개월 내 0 도달, 무한 decay 꼬리 없음.
- `weight_history.py`의 EMA 메모리 개념(`blend_ema`, prev=메모리) — prev는 이제 **직전 배포 가중치**.

### 3.4 신규 config (`PIPELINE_PARAMS`)
```python
"turnover_step": 0.01,       # 월 최대 이동폭 (1%p)
"turnover_deadband": 0.003,  # 데드밴드 (0.3%p)
```

---

## 4. 아키텍처 / 변경 파일

| 파일 | 변경 |
|---|---|
| `service/pipeline/smoothing.py` | `update_smoothing_memory`/`deploy_weights` 제거, **`step_smooth(target, prev, step, deadband, months)` 신규**. (OOS 가용 factor 정규화용 작은 헬퍼 `renorm_to(weights, factors)`는 유지/신규) |
| `service/pipeline/weight_history.py` | `blend_ema` 제거. save 함수는 prev=직전배포 기준으로 의미 정리(컬럼명: prev/target/deployed). |
| `service/pipeline/model_portfolio.py` `[6.5]` | `step_smooth` 호출. **prev = 직전 배포 가중치** 로딩(`factor_weights_{prev}.csv`). deployed 결과를 `weights_tbl` 로 — **단, 탈락 factor 포함** 위해 sim_factors 를 deployed 전체로 구성. |
| `[7] _construct_and_export` | sim_factors(=deployed)에 **탈락 factor 포함**. 종목단위 구성은 `filtered_data`/`kept_abbrs`에서 조회 → **탈락 factor가 당월 `kept_abbrs`에 있어야 종목 구성 가능**. 없으면 그 factor는 배포에서 drop(작은 불가피 turnover) + warning. |
| `service/backtest/walk_forward_engine.py` | blend → `step_smooth(target, cached_weights, step, deadband, months=weight_rebal_months)`. `cached_weights`가 곧 배포(메모리 구분 없음). OOS는 가용 factor로 renorm(기존 `deploy_weights`→`renorm_to`). |
| `config.py` | `turnover_step`/`turnover_deadband` 추가, `turnover_smoothing_alpha`·`turnover_min_weight` 제거. |
| `main.py` | backtest `--turnover-step`/`--turnover-deadband`, `--turnover-alpha`/`--turnover-min-weight` 제거. |
| `tests/` | `test_smoothing.py` 전면 교체(step/deadband/exit/정규화/cadence/첫회차/exit-monotonic). weight_history 테스트 갱신. |

**핵심 구현 포인트 — 탈락 factor 종목 구성:** deployed에 포함된 탈락 factor는 당월 `filtered_data`(sector-filter 통과 ~242개)에 있어야 long/short 종목 비중을 만들 수 있다. 최근 탈락 factor는 대개 통과하지만, 통과 못 하면 해당 factor를 deployed에서 제외하고 warning 로그(불가피).

---

## 5. 예상 효과 (트레이드오프)

- **배포 factor 수 증가**: 현재 선정(~37) + 청산 중(최근 ~3개월 탈락분). 선정 churn에 따라 **~50-70개** 예상. 무한 아님(~3개월 bound). 검증에서 실측.
- **5월 산출물 변경**: 5월 탈락 10개가 1.63%로 **배포에 포함**(직전 EMA 버전은 0). 5월 mp 재생성 필요.
- **turnover**: 유지 factor는 데드밴드로 거래 0, 탈락/신규는 ≤1%p/월 → 실거래 turnover 대폭 감소(검증에서 수치 확인).

---

## 6. 엣지케이스

| 상황 | 처리 |
|---|---|
| 첫 회차 (prev=None) | 목표 그대로 배포 |
| 유지 factor, gap<0.3%p | 고정(미세 흡수 renorm만) |
| 탈락 factor가 1%p 이하로 남음 | 다음 회차 0 도달 후 제거 |
| 순유출 월(탈락>신규, 흡수처 부족) | 탈락분 고정, 잔여를 held+movers가 흡수(held가 정상보다 크게 움직일 수 있음 — 드묾) |
| 탈락 factor가 당월 kept_abbrs에 없음 | 배포 제외 + warning (종목 구성 불가) |
| months>1 (백테스트) | max_step = step×months |

---

## 7. 검증 (CLAUDE.md 프로세스 + 사용자 요청)

### A. 단위 테스트 (`tests/test_unit/test_smoothing.py`)
`step_smooth`: 첫회차 / 데드밴드 고정 / 1%p 이동 / 탈락 0쪽 이동 & 절대 증가 안 함(monotonic) / 합=1.0 / months 스케일 / 순유출월.

### B. test 모드 mp diff
의도된 변경(배포 비중 산식 교체) — 기대 diff 문서화.

### C. production 재실행 `mp 2026-05-31`
- 배포 합 = 1.0, 배포 factor 수(현재+청산중) 확인.
- 5월 탈락 factor가 ~1.63%로 배포 포함 확인.
- 유지 factor 변동이 작은지(데드밴드 작동) 확인.

### D. **OOS 백테스트 — 기존 결과들과 비교 (사용자 요청)**
`combo_18_0.1` 신규(절대스텝) 실행 후, **기존 커밋된 결과와 비교**:
1. **직전 EMA+renorm 버전** (현재 main-ikm 커밋, Sharpe 0.8026 / CAGR 1.66% / MDD -2.86% / Calmar 0.58) — 주 비교 대상.
2. (선택) **무스무딩(`--turnover-step` 매우 크게=즉시 목표)** 대비, 스무딩 효과 맥락.
- 비교 항목: OOS Sharpe/CAGR/MDD/Calmar/Funnel + **배포 turnover**(아래).

### E. **배포 turnover 측정 (핵심 — 진짜 목표 확인)**
신규 설계에선 `result.weight_history` = 배포 가중치(메모리 구분 없음)이므로 `compute_avg_turnover`가 **실거래 turnover**를 올바르게 측정. 절대스텝 vs EMA버전 turnover를 **수치 비교**하여 실제 감소를 확인. (필요시 backtest 결과에 turnover 컬럼/지표 노출 추가.)

### F. 회귀
`pytest tests/test_unit/ -v` 전체 통과.

---

## 8. 리스크
- **탈락 factor 종목 구성 의존성(§4)**: 당월 kept_abbrs에 없으면 배포 불가 → drop. 빈도는 검증에서 확인.
- **OOS 성과 변화**: 배포 비중이 EMA버전과 달라져 OOS가 바뀜. D에서 열화 없는지 확인(특히 Sharpe). 크게 나빠지면 step/deadband 재검토.
- **배포 수 증가**: ~50-70. 과하면 step 상향(청산 가속, turnover와 trade)으로 조절 — config 노출로 코드변경 불필요.
- **순유출월 deadband 완화**: 드문 경우 held가 정상보다 움직임. 테스트로 동작 명시.
