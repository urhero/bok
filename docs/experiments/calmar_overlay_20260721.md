# 포트폴리오 레벨 Vol Targeting 오버레이 연구 (2026-07-21)

- 브랜치: `feature/calmar-overlay`
- 스크립트: `research/calmar_overlay_study.py` (그리드), `research/calmar_overlay_robustness.py` (견고성)
- 목표: 전략 조작/오버레이로 Calmar(또는 Sharpe·MDD) 개선
- 판정: **vol targeting(w=12) 채택 후보** — 전 하위구간 개선, 파라미터 프리 변형도 유효

## 아이디어

Constrained EW의 월간 노출을 t-1까지의 전략 실현변동성에 반비례해 스케일:
`k_t = min(cap, target_vol / realized_vol_{t-1, w=12M})`, 오버레이 수익률 = `k_t x r_t`.
변동성 군집(vol clustering) 때문에 시끄러운 구간의 노출을 줄이면 MDD가 줄고
위험조정 수익이 개선된다는 표준 가설. **k_t 는 t-1 정보만 사용 -> 사후 스케일링
계산이 곧 walk-forward 백테스트와 동일 (look-ahead 없음, 백테스트 재실행 불필요).**

## 결과 (OOS 전기간, baseline: CAGR 1.88% / MDD -10.2% / Sharpe 0.516 / Calmar 0.184)

| 오버레이 | CAGR | MDD | Sharpe | Calmar |
|----------|------|-----|--------|--------|
| vol_target t=3%, w=12, cap=1.5 | 2.22% | -8.6% | 0.618 | 0.258 |
| **adaptive(파라미터 프리), w=12, cap=1.5** | 2.21% | -9.0% | 0.606 | 0.246 |
| adaptive, w=12, cap=1.0 (무레버리지) | 1.75% | -7.9% | 0.547 | 0.222 |
| vol_target t=4%, w=12, cap=1.0 (무레버리지) | 1.87% | -8.9% | 0.546 | 0.210 |
| dd_derisk(낙폭 3%시 노출 0) | 1.40% | -5.6% | 0.470 | 0.253 |

- adaptive = 타깃을 고정 상수 대신 **확장창 중위 실현변동성**으로 (고정 타깃 파라미터 제거)
- w=6은 전 케이스 악화 -> 12개월 창이 유효 (단기 vol 추정 노이즈)
- dd_derisk는 Calmar만 개선, Sharpe 악화 -> 열위

## 견고성 (과적합 체크)

전반(~2019-09)/후반(2019-09~)/위험구간(2023~) 분해 — vt t=3% cap1.5 및 adaptive cap1.5는
**모든 하위구간에서 Sharpe·Calmar·MDD 동시 개선**:

| span | baseline Sharpe/Calmar | vt3% Sharpe/Calmar | adaptive1.5 Sharpe/Calmar |
|------|------------------------|--------------------|---------------------------|
| 전반 | 0.417 / 0.223 | 0.530 / 0.329 | 0.506 / 0.279 |
| 후반 | 0.599 / 0.252 | 0.693 / 0.329 | 0.690 / 0.316 |
| 2023~ | 1.013 / 0.811 | 1.050 / 1.334 | 1.059 / 1.346 |

- 파라미터 프리 adaptive가 튜닝된 고정 타깃과 대등 -> **타깃 파라미터 과적합 아님**
- 무레버리지(cap=1.0)는 개선 폭이 작고 전반 구간 Sharpe 소폭 열위 (디레버만 가능해
  조용한 구간의 수익 기회 상실) — 그래도 full 기준 4지표 중 3지표 개선

## 확장 그리드 (2026-07-21 2차: 타깃 2~6% x 창 6/9/12/18/24M x cap 1.0/1.5/2.0)

- **창 9개월 이상 영역 전체가 개선** (6M만 악화) -> 넓은 robust 고원, 특정 파라미터
  의존 아님. 상위권:

| 설정 | CAGR | MDD | Sharpe | Calmar |
|------|------|-----|--------|--------|
| **adaptive w=18, cap=2.0** | 2.29% | -8.1% | 0.625 | **0.283** |
| vol_target 4%, w=18, cap=2.0 | 2.84% | -10.3% | 0.617 | 0.276 |
| adaptive w=18, cap=1.5 | 2.21% | -8.1% | 0.613 | 0.274 |
| adaptive w=24, cap=2.0 | 2.09% | -7.6% | 0.590 | 0.275 |
| (1차 최고) vol_target 3%, w=12, cap=1.5 | 2.22% | -8.6% | 0.618 | 0.258 |

- 하위구간(전/후반, 2023~) 분해에서도 adaptive w=18 cap 1.5/2.0, vt 4% w=18 cap 2.0
  모두 전 구간 Sharpe/Calmar >= baseline (risk 구간 Calmar 0.81 -> 1.07~1.09).
- cap 2.0의 추가 이득은 1.5 대비 소폭 (Sharpe +0.01) — cap은 1.5로도 충분.
- adaptive(파라미터 프리)가 각 (창, cap)에서 튜닝된 고정 타깃과 대등 이상 -> 최종
  후보는 **adaptive w=18, cap=1.5** (남는 파라미터: 창, cap 두 개뿐).

## 채택 시 결정 사항 (사용자)

1. **cap (레버리지 허용 여부)**: 1.5 허용 시 개선 극대화 (그로스 노출 최대 1.5배 필요,
   Bloomberg 틸트 스케일 확대). 1.0(디레버 전용)은 보수적이지만 개선 폭 축소.
2. **타깃**: adaptive(파라미터 프리) 권장 — 고정 상수 튜닝 회피.
3. 프로덕션 구현: mp 출력 가중치에 k_t 곱하기 (k_t 는 walk-forward 결과 CSV의
   trailing 12M 실현변동성으로 계산, config `exposure_overlay` 플래그로 on/off).

## 재현

```bash
python research/calmar_overlay_study.py       # 그리드 전체
python research/calmar_overlay_robustness.py  # 하위구간 + adaptive
```
