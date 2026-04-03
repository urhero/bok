# BOK 파이프라인 Evaluation & Backtesting 구현 계획

> 작성일: 2026-04-02
> 대상: BOK 중국 A주(MXCN1A) 팩터 기반 모델 포트폴리오 파이프라인
> 목적: 기존 파이프라인 코드를 수정하지 않고, Walk-Forward 백테스트 레이어를 씌워 과적합 진단 및 벤치마크 비교 기능 추가

---

## 0. 현재 시스템 이해 (필수 선행)

### 0.1 반드시 먼저 읽을 파일

구현 전에 `research.md`를 반드시 정독하라. 이 문서에 파이프라인의 모든 단계([1]~[7]), 데이터 흐름, 숨겨진 규칙, 건드리면 안 되는 로직이 기술되어 있다.

### 0.2 파이프라인 구조 요약

```
main.py → ModelPortfolioPipeline.run()
  [1] _load_data + _prepare_metadata    → raw_data, mreturn_df
  [2] _analyze_factors                   → factor_stats (5분위 분석)
  [3] filter_and_label_factors           → filtered_data (L/N/S 라벨)
  [4] _evaluate_universe                 → ret_df (상위 50 팩터 수익률 행렬), meta, negative_corr
  [5] _optimize_mixes                    → 2-팩터 믹스 (그리드 서치)
  [6] simulate_constrained_weights       → 최적 가중치 (MC 100만 시뮬레이션)
  [7] _construct_and_export              → Bloomberg용 CSV
```

### 0.3 핵심 제약

- **데이터 기간: ~70개월** (약 2018~2024). 20년이 아니다. 이 짧은 기간이 모든 설계를 좌우한다.
- **[1]~[7] 단계는 순서 불변**. [3]은 [2]의 결과를 쓰고, [4]는 [3]의 결과를 쓰는 식으로 체인되어 있다.
- **건드리면 안 되는 로직**: 1개월 래그(shift(1)), hardcoded 가중치 모드, 스타일 캡 25%, 거래비용 30bp, sort_order 방향 통일. (research.md §4.1 참조)
- **기존 모듈의 내부 로직은 한 줄도 수정하지 않는다.** 오직 외부에서 감싸는(wrapper) 방식으로만 구현한다.

### 0.4 과적합 위험 지점

현재 파이프라인에서 과적합이 발생하는 **진짜** 위치:

| 단계 | 과적합 위험 | 이유 |
|------|------------|------|
| [4] 상위 50 팩터 선정 | **높음** | 전체 기간 CAGR 순위로 선정. "미래를 보고 고른 팩터" |
| [5] 2-팩터 믹스 그리드 서치 | **높음** | 전체 기간 수익률로 101포인트 탐색. IS 최적화 |
| [6] MC 시뮬레이션 | 중간 | 스타일 캡 25% + Dirichlet 제약으로 자유도가 낮아 과적합 여지 제한적 |
| [2] 5분위 분석 | 낮음 | 횡단면 정렬이라 시계열 과적합 아님. 단, IS 범위는 제한 필요 |

→ [4]와 [5]가 핵심. MC([6])는 주범이 아니다.

### 0.5 백테스트 레벨 결정: Factor-Level

본 백테스트는 **팩터 수익률 레벨**에서 수행한다. 종목(stock-level) MP까지 내려가지 않는다.

```
Factor-Level Backtest (채택):
  1. IS에서 [2]~[3]으로 L/N/S 라벨 규칙 학습
  2. IS에서 [4]로 팩터 수익률 행렬 생성 (30bp 거래비용 이미 포함)
  3. IS에서 [5]~[6]으로 팩터 가중치 최적화
  4. OOS 월에 IS 규칙을 적용하여 팩터 수익률 계산
  5. OOS 팩터 수익률 × 팩터 가중치 = OOS 포트폴리오 수익률

Stock-Level Backtest (미채택):
  - [7]까지 돌려 종목별 가중치를 매월 산출
  - 종목 레벨 턴오버, 거래비용 별도 계산
  - 복잡도 높고, 초기 검증에 불필요
```

**핵심 결정: 거래비용 이중 적용 금지.**
기존 `calculate_vectorized_return()`이 팩터 내부 종목 리밸런싱에서 30bp를 이미 차감한다.
팩터 가중치 변경 시 추가 30bp를 적용하면 이중 과금이 된다.
→ OOS 수익률 계산 시 팩터 가중치 변경에 대한 별도 거래비용은 적용하지 않는다.
→ 팩터 수익률 자체가 이미 net-of-cost이므로, factor_return × factor_weight의 가중합이 포트폴리오 수익률이 된다.

### 0.6 simulation 모드 통일

백테스트 전체에서 simulation 모드만 사용한다. hardcoded 모드는 프로덕션 전용이므로 백테스트에 포함하지 않는다. Step 0의 IS 기준값과 Step 2의 Deflation Ratio 분모도 모두 simulation 모드의 결과여야 한다. hardcoded 성과와 비교하면 apples-to-oranges가 된다.

---

## 1. 구현 범위 및 순서

총 3개 Step을 순서대로 구현한다. 각 Step은 독립적으로 동작 가능하며, 이전 Step의 결과를 활용한다.

```
Step 0: 벤치마크 비교 모듈          ← 즉시 구현, 기존 코드 변경 0
Step 1: Expanding Window 엔진      ← 핵심 구현
Step 2: 과적합 진단 모듈            ← Step 1 결과 소비
```

---

## 2. Step 0: 벤치마크 비교 모듈

### 2.1 목적

현재 simulation 모드로 산출한 최적화 가중치(MC 기반)가 **단순 동일가중(1/N)**을 이기는지 확인한다. 이기지 못하면 Walk-Forward를 만들 이유가 없다.

**중요 한계**: 이 Step 0은 전체 기간(IS) 데이터에서의 비교이므로, Sanity Check(최적화기가 바보짓을 하지 않는지 확인)일 뿐이다. **진짜 벤치마크 비교는 Step 1의 OOS 구간에서 수행**해야 한다. Step 1의 `result_stitcher.py`에서 OOS 동일가중 수익률을 병행 생성하여 OOS MP 수익률과 비교하는 것이 핵심이다.

### 2.2 파일 구조

```
service/pipeline/benchmark_comparison.py   (신규)
```

### 2.3 입력/출력

- **입력**: `ret_df` (팩터별 수익률 행렬, [4] _evaluate_universe의 산출물), `sim_weights` ([6]의 산출물)
- **출력**: 비교 리포트 dict + 콘솔 출력 (Rich 테이블)

### 2.4 구현 상세

```python
def create_equal_weight_benchmark(ret_df: pd.DataFrame) -> dict:
    """
    ret_df: [4]에서 산출된 팩터별 수익률 행렬 (Date × Factor)
            첫 행은 0.0 (기준점). research.md §2.2 [4] 참조.
    
    반환:
      - return_series: 동일가중 월간 수익률 Series
      - cumulative: 누적 수익률 Series
      - cagr: 연환산 수익률 (파이프라인과 동일한 CAGR 공식 사용)
      - mdd: 최대 낙폭
    
    CAGR 공식 (research.md 부록 참조):
      months = len(ret_df) - 1  (첫 행 기준점 제외)
      CAGR = cumulative_return^(12/months) - 1
    """
```

```python
def create_mp_portfolio_return(ret_df: pd.DataFrame, weights: dict) -> dict:
    """
    simulation 모드([6])에서 산출된 팩터 가중치를 적용한 MP 수익률 계산.
    
    weights: {factor_abbr: weight} 형태. [6] simulate_constrained_weights의 결과.
    ret_df의 컬럼명과 weights의 키가 매칭되어야 함.
    
    가중 수익률 = sum(ret_df[factor] * weights[factor] for factor in weights)
    """
```

```python
def compare_vs_benchmark(ret_df, weights) -> dict:
    """
    MP vs. 동일가중 비교 리포트 생성.
    
    출력 항목:
      - mp_cagr, ew_cagr, excess_cagr
      - mp_mdd, ew_mdd
      - mp_sharpe, ew_sharpe (무위험수익률=0 가정, 또는 config에서 주입)
      - win_rate: MP가 동일가중을 이긴 월 비율
      - t_statistic, p_value: 월간 초과수익의 t-검정 (scipy.stats.ttest_1samp)
    
    ⚠️ avg_monthly_turnover는 제외:
      factor-level 백테스트에서 팩터 가중치 변경의 turnover는
      기존 파이프라인에 정의되어 있지 않다.
      turnover/거래비용은 이미 각 팩터 수익률 내부에 반영되어 있다 (§0.5).
    """
```

### 2.5 통합 방법

`model_portfolio.py`의 `run()` 메서드를 수정하지 않는다. 대신:

1. `main.py`에 `python main.py mp <start> <end> --benchmark` 옵션 추가
2. 이 옵션이 켜지면, **simulation 모드를 명시적으로 지정**하여 파이프라인을 실행한다.
   research.md 기준 [6]의 기본 모드는 hardcoded이므로, `--benchmark` 실행 시
   반드시 mode="simulation"으로 [6]을 호출해야 한다.
   `pipeline.run()` 후 `pipeline.ret_df`를 꺼내는 방식은 `self.ret_df`가
   인스턴스 변수로 노출된다는 보장이 없으므로 위험하다.
   → 안전한 방법: pipeline의 [1]~[4]를 순수 함수로 실행하여 ret_df를 직접 얻고,
     [6]을 simulation 모드로 호출하여 weights를 얻은 뒤 비교하는 별도 경로.
   → 또는 pipeline.run()의 반환값/속성을 코드에서 확인 후 결정.
3. 결과를 Rich 테이블로 콘솔 출력 + `output/benchmark_comparison.csv` 저장

### 2.6 주의사항

- `ret_df`의 첫 행이 0.0인 것을 감안하여 CAGR 계산 시 `months = len(ret_df) - 1` 사용
- `simulate_constrained_weights(mode="simulation")`의 반환값 형태를 확인 — `research.md §2.2 [6]`에서 가중치 구조(dict 또는 array) 확인 후 사용. ⚠️ `pipeline.weights`가 인스턴스 속성으로 노출되는지 코드에서 확인할 것 (research.md에 명시적 보장 없음).
- 동일가중 벤치마크는 [4]에서 선정된 상위 50개 팩터에 대해서만 계산 (유니버스 동일). `(ret_df == 0).sum() <= 10` 필터를 통과한 팩터만 포함되어야 함을 코드에서 명시적으로 확인할 것.
- **턴오버 참고 (내부 팩터 레벨)**: 이 턴오버는 팩터 가중치 변경이 아니라, 각 팩터 내부의 종목 리밸런싱에서 발생하는 비용이다. 기존 `calculate_vectorized_return()`은 리밸런싱 블록별 누적 성장률로 drift를 계산한 뒤 `turnover = |new_weight - drifted_weight|`를 산출하고 30bp를 차감한다. ret_df의 수익률은 이미 이 비용이 반영된 net-of-cost 값이다.

---

## 3. Step 1: Expanding Window (Walk-Forward) 엔진

### 3.1 목적

파이프라인 전체를 시간 순서대로 잘라가며 반복 실행하여, **IS 데이터만으로 팩터 선정과 가중치를 결정하고 OOS 1개월 수익률을 기록**하는 "의사 실전" 시뮬레이션을 구현한다.

### 3.2 파일 구조

```
service/backtest/
  ├── walk_forward_engine.py      (오케스트레이터)
  ├── data_slicer.py              (날짜 기반 데이터 필터링)
  └── result_stitcher.py          (OOS 결과 접합 + 성과 계산)
```

`main.py`에 신규 커맨드 추가:
```
python main.py backtest <start> <end> [--factor-rebal-months 6] [--weight-rebal-months 3] [--min-is-months 36]
```

### 3.3 핵심 설계: 계층적 리밸런싱

70개월 데이터에서 [2]~[6] 전체를 매월 재실행하면 실행 시간이 과도하다 (1~3분 × ~30회 = 30~90분). 따라서 **리밸런싱 주기를 단계별로 차등 적용**한다.

```
Tier 1: 팩터 규칙 학습 + 팩터 수익률 사전 계산 ([2]~[3] + aggregate)
  - 주기: 6개월마다 (기본값, 파라미터로 조정 가능)
  - 실행:
    1. [2]~[3] IS 데이터로 5분위 분석 → 섹터 필터 → L/N/S 규칙 학습 → rule_bundle
    2. rule_bundle을 **전체 가용 데이터**(IS+향후 OOS 포함)에 적용(transform)
    3. aggregate_factor_returns를 1회 실행 → precomputed_ret_df (전기간 팩터 수익률)
  - 산출물: rule_bundle + precomputed_ret_df
  - ⚠️ look-ahead가 아닌 이유:
    규칙(L/N/S)은 IS에서만 학습(fit).
    팩터 수익률은 각 월의 횡단면 데이터를 규칙에 기계적으로 적용(transform)한 결과.
    calculate_vectorized_return의 drift/turnover도 시계열 순서대로 계산되므로
    전기간 1회 계산과 월별 확장 계산의 결과가 동일.

Tier 2: 팩터 선정 + 가중치 최적화 ([4] 후반 + [5]~[6])
  - 주기: 3개월마다 (기본값)
  - 실행:
    [4] precomputed_ret_df에서 IS 구간만 슬라이스 → ret_df_is
        CAGR 계산 → 상위 팩터 선정 → meta, negative_corr
    [5] meta + negative_corr로 2-팩터 믹스
    [6] MC 시뮬레이션 → 팩터 가중치
  - ⚠️ aggregate_factor_returns 재실행 불필요 (Tier 1에서 사전 계산 완료)

Tier 3: OOS 적용 (매월)
  - 주기: 매월
  - 실행: precomputed_ret_df에서 OOS 월 행을 조회 → 팩터별 수익률 추출
          portfolio_return = sum(weight[f] * precomputed_ret_df[oos_date][f])
          ew_return = mean(precomputed_ret_df[oos_date][f])
  - ⚠️ aggregate_factor_returns 재실행 불필요. 조회만 (밀리초 단위).
```

**이전 설계 대비 성능 개선**:
```
이전: Tier 3에서 매월 aggregate_factor_returns를 확장 데이터로 재실행 (34회)
     → O(n²) 성격, 25~40분 예상, 구조적 병목
수정: Tier 1에서 1회만 전기간 팩터 수익률을 사전 계산 (~6회, 6개월마다)
     → Tier 2는 슬라이스만, Tier 3는 조회만, 전체 ~5분 이내
```

**rule_bundle의 정의 (Tier 1의 핵심 산출물)**:

[2]~[3]이 학습한 **규칙만** 캡슐화한 객체. OOS와 Tier 2에서 동일한 규칙을 적용하기 위해 필요.

```python
rule_bundle = {
    # [2]에서 학습/추출
    'kept_abbrs': list,           # 유효 팩터 약어 목록
    'factor_stats': list,         # 팩터별 5분위 분석 결과
    'sort_order_map': dict,       # 팩터별 정렬 방향 {factor_abbr: 0 or 1}
                                  # 0=낮을수록 좋음 (PER 등), 1=높을수록 좋음 (ROE 등)
                                  # OOS transform 시 val_lagged *= -1 적용에 필요
    
    # [3]에서 학습  
    'dropped_sectors': dict,      # 팩터별 음의 스프레드로 제거된 섹터 목록
                                  # {factor_abbr: [sector1, sector2, ...]}
    'label_rules': dict,          # 팩터별 분위→L/N/S 매핑
                                  # {factor_abbr: {Q1: 'L', Q2: 'L', Q3: 'N', Q4: 'S', Q5: 'S'}}
    'threshold_pct': float,       # L/N/S 확장 임계값 (기본 0.10, research.md §2.2 [3])
                                  # PIPELINE_PARAMS에서 가져옴. OOS 재적용 시 동일 값 사용.
}
# ⚠️ filtered_data(DataFrame)는 rule_bundle에 포함하지 않는다.
# 이유: rule_bundle은 Tier 1 시점의 IS로 학습되지만, Tier 2는 더 최신 IS에서
#       실행된다. filtered_data를 캐시하면 Tier 2가 stale 데이터를 사용하게 된다.
# 대신: 매 Tier 2 실행 시 rule_bundle의 규칙을 현재 IS 데이터에 재적용(transform)하여
#       최신 filtered_data를 생성한다.
```

**규칙 적용(transform) 헬퍼 함수가 필요**:

```python
def apply_rules_to_data(data, rule_bundle) -> pd.DataFrame:
    """
    Tier 1에서 학습한 규칙을 데이터에 적용하여 filtered_data를 생성.
    fit(학습)이 아닌 transform(적용)이다.
    
    Tier 1에서 호출 시: data = raw_data_full (전체 데이터)
      → 전기간 filtered_data 생성 → aggregate_factor_returns에 전달
    
    1. kept_abbrs에 해당하는 팩터만 필터
    2. sort_order_map으로 방향 정규화
    3. dropped_sectors 규칙으로 해당 섹터 제거
    4. label_rules + threshold_pct로 L/N/S 라벨 적용
    
    ⚠️ 구현 시 cross-sectional 순위 주의:
      5분위 분석은 groupby(["ddt", "sec"]) — 즉 특정 월 × 특정 섹터 내에서의
      횡단면적 순위(rank)를 구한다 (research.md §2.2 [2]).
      전체 시계열 기준으로 순위를 매기면 시계열 정보가 누수되므로,
      반드시 월별/섹터별 그룹 단위로 rank → percentile → quantile → L/N/S를
      적용해야 한다. calculate_factor_stats_batch()가 이미 이 구조로 되어 있으므로
      해당 함수를 직접 호출하되, IS에서 학습한 label_rules를 적용하는 transform
      단계에서도 동일한 그룹 구조를 유지할 것.
    
    반환: 규칙이 적용된 filtered_data
    """
```

**이전 설계와의 차이**:
- 이전: Tier 1 = [2]~[4], Tier 2 = [5]~[6]
- 수정: Tier 1 = [2]~[3] (규칙 학습), Tier 2 = [4]~[6] (적용 + 최적화)
- 이유: [5]가 [4]의 meta/ret_df/negative_corr에 직접 의존하므로 
        [4]를 Tier 1에 넣으면 Tier 2가 최신 IS 데이터를 반영하지 못함

### 3.4 구현 상세

#### 3.4.1 data_slicer.py

```python
def slice_data_by_date(raw_data: pd.DataFrame, 
                       mreturn_df: pd.DataFrame, 
                       end_date: str) -> tuple:
    """
    전체 데이터에서 end_date **이하** (<=) 데이터만 추출.
    
    ⚠️ 경계 조건: <= end_date (inclusive)
    - end_date 월의 데이터를 IS에 포함한다.
    - OOS 월은 end_date 다음 월이다.
    - 예: end_date='2021-12-31'이면 2021-12까지 IS, 2022-01이 OOS.
    - < (strict less than)로 구현하면 IS가 의도보다 1개월 짧아지는 
      off-by-one 버그가 발생하므로 반드시 <= 로 구현할 것.
    
    ⚠️ 반드시 .copy()를 호출할 것.
    pandas의 boolean indexing은 view를 보장하지 않으며,
    후속 함수가 DataFrame에 파생 변수를 할당할 때 원본이 오염될 수 있다.
    
    구현:
      sliced_raw = raw_data[raw_data['ddt'] <= end_date].copy()
      sliced_mret = mreturn_df[mreturn_df['ddt'] <= end_date].copy()
    
    반환: (sliced_raw_data, sliced_mreturn_df)
    """
```

```python
def get_oos_dates(all_dates: list, min_is_months: int) -> list:
    """
    OOS 시작점 이후의 모든 월말 날짜 반환.
    
    all_dates: raw_data['ddt'].unique() 정렬된 리스트
    min_is_months: 최소 IS 기간 (기본 36개월)
    
    반환: OOS 대상 날짜 리스트
    """
```

#### 3.4.2 walk_forward_engine.py

```python
class WalkForwardEngine:
    """
    기존 ModelPortfolioPipeline을 감싸는 Walk-Forward 오케스트레이터.
    
    핵심 원칙:
      1. 기존 파이프라인 모듈의 내부 코드를 수정하지 않는다.
      2. 데이터를 메모리에 1회만 로드하고 날짜 필터로 IS 범위를 제어한다.
      3. 계층적 리밸런싱으로 실행 시간을 최적화한다.
      4. Factor-level backtest: 팩터 수익률(net-of-cost) × 팩터 가중치.
         팩터 가중치 변경에 대한 별도 거래비용은 적용하지 않는다 (§0.5 참조).
    """
    
    def __init__(self, 
                 min_is_months=36,
                 factor_rebal_months=6,
                 weight_rebal_months=3,
                 turnover_smoothing_alpha=1.0):
        """
        min_is_months: 최소 IS 기간. 36개월이면 OOS는 37번째 월부터.
        factor_rebal_months: Tier 1 리밸런싱 주기 (규칙 학습 + 사전 계산).
        weight_rebal_months: Tier 2 리밸런싱 주기 (팩터 선정 + 가중치).
        turnover_smoothing_alpha: EMA 가중치 블렌딩 비율 (0~1).
          1.0 (기본값) = 스무딩 없음. 새 가중치를 그대로 적용.
          0.5 = 새 가중치 50% + 이전 가중치 50%.
          낮을수록 턴오버가 줄어들고 가중치 변화가 완만해짐.
          
          ⚠️ 과적합 진단(Step 2)에서는 alpha=1.0(기본값)을 사용할 것.
          스무딩을 적용하면 최적화 로직의 순수한 예측력을 측정할 수 없다.
          alpha < 1.0은 운용 시뮬레이션(production realism) 목적으로만 사용.
        """
    
    def run(self, start_date, end_date) -> WalkForwardResult:
        """
        의사 코드:
        
        1. 데이터 1회 로딩
           # ⚠️ [1]은 _load_data + _prepare_metadata의 결합 단계.
           # _load_data()만으로는 factor_info merge, M_RETURN merge가 미완성.
           # pipeline-ready parquet은 이미 factorOrder 포함이므로
           # _prepare_metadata가 merge를 생략할 수 있지만, M_RETURN merge는 필수.
           pipeline = ModelPortfolioPipeline()
           raw_data, mreturn_df, _, _ = pipeline._load_data(start_date, end_date)
           pipeline._prepare_metadata()
           raw_data = pipeline.raw_data  # metadata 병합 완료
           all_dates = sorted(raw_data['ddt'].unique())
           oos_dates = get_oos_dates(all_dates, self.min_is_months)
        
        2. 캐시 초기화
           cached_rule_bundle = None         # Tier 1: 규칙
           precomputed_ret_df = None         # Tier 1: 전기간 팩터 수익률 (핵심 캐시)
           cached_weights = None             # Tier 2: 팩터 가중치
           cached_meta = None               # Tier 2: IS meta (rank corr 진단용)
           cached_selected_factors = None    # Tier 2: 선정된 팩터 목록
        
        3. OOS 루프
           for i, oos_date in enumerate(oos_dates):
               # 날짜 정의 (off-by-one 방지):
               #   oos_date = all_dates[min_is_months + i] (OOS 대상 월)
               #   is_end_date = all_dates[min_is_months + i - 1] (IS 마지막 월)
               #   slice_data_by_date(..., is_end_date) 는 <= is_end_date로 필터
               is_end_idx = min_is_months + i - 1
               is_end_date = all_dates[is_end_idx]
               
               # ── Tier 1: 규칙 학습 + 팩터 수익률 사전 계산 ──
               if cached_rule_bundle is None or i % factor_rebal_months == 0:
                   is_raw, is_mret = slice_data_by_date(raw_data, mreturn_df, is_end_date)
                   # .copy() 내부 수행 — §3.4.1
                   
                   # [2]~[3] 규칙 학습 (IS 데이터만)
                   cached_rule_bundle = run_rule_learning(is_raw)
                   
                   # 전체 데이터에 규칙 적용 + aggregate_factor_returns 1회 실행
                   # → 전기간 팩터 수익률을 사전 계산
                   full_filtered = apply_rules_to_data(raw_data, cached_rule_bundle)
                   precomputed_ret_df = pipeline.aggregate_factor_returns(
                       full_filtered, cached_rule_bundle['kept_abbrs'], ...
                   )
                   # precomputed_ret_df: (전체 월 × 전체 유효 팩터) 수익률 행렬
                   # ⚠️ look-ahead 아닌 이유: §3.3 참조
               
               # ── Tier 2: 팩터 선정 + 가중치 ──
               if cached_weights is None or i % weight_rebal_months == 0:
                   # precomputed_ret_df에서 IS 구간만 슬라이스 (aggregate 재실행 불필요)
                   ret_df_is = precomputed_ret_df[precomputed_ret_df.index <= is_end_date]
                   ret_df_is.iloc[0] = 0.0  # 기준점
                   valid = ret_df_is.columns[(ret_df_is == 0).sum() <= 10]
                   ret_df_is = ret_df_is[valid]
                   
                   # CAGR → 상위 팩터 선정 → meta, negative_corr
                   months = len(ret_df_is) - 1
                   cagr = ((1 + ret_df_is).cumprod().iloc[-1] ** (12 / months) - 1)
                   # ... meta 구성, top_factors 선정
                   neg_corr = calculate_downside_correlation(ret_df_is)
                   
                   cached_selected_factors = ...  # 선정된 팩터 목록
                   cached_meta = ...              # IS meta (CAGR 순위 포함)
                   
                   # [5]~[6]
                   raw_new_weights = run_weight_optimization(
                       ret_df_is, cached_meta, neg_corr
                   )
                   
                   # EMA 가중치 블렌딩 (턴오버 스무딩)
                   if turnover_smoothing_alpha >= 1.0 or cached_weights is None:
                       cached_weights = raw_new_weights
                   else:
                       # blended[f] = new[f]*α + old[f]*(1-α), 정규화
                       alpha = turnover_smoothing_alpha
                       all_factors = set(raw_new_weights) | set(cached_weights)
                       blended = {f: raw_new_weights.get(f,0)*alpha 
                                    + cached_weights.get(f,0)*(1-alpha) 
                                  for f in all_factors}
                       total = sum(blended.values())
                       cached_weights = {f: w/total for f, w in blended.items()}
               
               # ── Tier 3: OOS 1개월 팩터 수익률 (조회만) ──
               oos_factor_returns = precomputed_ret_df.loc[oos_date, cached_selected_factors]
               oos_return = (oos_factor_returns * pd.Series(cached_weights)).sum()
               oos_ew_return = oos_factor_returns.mean()
               
               results.append({
                   'date': oos_date,
                   'oos_return': oos_return,
                   'oos_ew_return': oos_ew_return,
                   'oos_factor_returns': oos_factor_returns.to_dict(),
                   'weights': cached_weights.copy(),
                   'is_meta': cached_meta.copy(),
                   'rule_bundle': cached_rule_bundle.copy() if (i % factor_rebal_months == 0) else None,
                   'is_rule_rebal': (i % factor_rebal_months == 0),
                   'is_weight_rebal': (i % weight_rebal_months == 0),
               })
        
        4. 결과 반환
           return WalkForwardResult(results)
        """
```

#### 3.4.3 파이프라인 내부 함수 호출 방법

기존 `ModelPortfolioPipeline`의 private 메서드(`_analyze_factors` 등)를 직접 호출하는 것은 캡슐화를 깨뜨린다.

**각 모듈의 순수 함수 + pipeline 인스턴스 메서드 조합으로 호출**

```python
# 기존 모듈의 public 순수 함수들:
from service.pipeline.factor_analysis import calculate_factor_stats_batch, filter_and_label_factors
from service.pipeline.correlation import calculate_downside_correlation
from service.pipeline.optimization import find_optimal_mix, simulate_constrained_weights
from service.pipeline.weight_construction import construct_long_short_df, calculate_vectorized_return

# ⚠️ aggregate_factor_returns는 model_portfolio.py 소속 (research.md §3.2).
# Pipeline 클래스의 메서드이므로 직접 import 불가.
# 호출 방식: A-1 확정 (pipeline 인스턴스를 통해 호출)
#   pipeline.aggregate_factor_returns(filtered_data, kept_abbrs, ...)
#   pipeline 인스턴스는 데이터 로딩 시 1회 생성하여 walk-forward 루프에서 재사용.
# A-2(로직 복제)는 drift 위험, A-3(코드 리팩터링)은 wrapper-only 원칙 위반.
```

**⚠️ 순수 함수 검증 (구현 전 필수 확인)**:
위 함수들이 진짜 순수 함수(부작용 없음, 전역 상태 미수정)인지 구현 전에 코드를 읽고 확인할 것. 만약 내부에서 글로벌 변수를 수정하거나, 파일을 쓰거나, 인자로 받은 DataFrame을 in-place 수정하는 경우가 있으면 래핑 시 `.copy()`를 넣어 방어해야 한다. research.md §3.2에 따르면 이 함수들은 외부 import가 없거나 `pipeline_utils`만 참조하므로 순수 함수일 가능성이 높지만, **가정하지 말고 확인할 것.**

**호출 순서 및 중간 처리 규칙**:
각 Walk-Forward 루프에서 순수 함수를 호출할 때 반드시 아래 순서와 규칙을 지킬 것:

Tier 1 (규칙 학습 + 사전 계산, 6개월마다):
1. `slice_data_by_date()` + `.copy()`로 IS 범위 데이터 추출
2. IS 데이터에 대해 `calculate_factor_stats_batch()` 호출 → IS 범위 내에서 shift(1) 적용
3. `filter_and_label_factors()` 호출 → L/N/S 규칙 학습
4. rule_bundle 구성 (kept_abbrs, sort_order_map, dropped_sectors, label_rules, threshold_pct)
5. `apply_rules_to_data(raw_data_full, rule_bundle)` → 전체 데이터에 규칙 적용
6. `pipeline.aggregate_factor_returns(full_filtered, kept_abbrs, ...)` → precomputed_ret_df
   ⚠️ A-1 방식 (pipeline 인스턴스 메서드). 전기간 1회 실행.

Tier 2 (팩터 선정 + 가중치, 3개월마다):
7. precomputed_ret_df에서 IS 구간 슬라이스: `ret_df_is = precomputed_ret_df[index <= is_end_date]`
   ⚠️ aggregate 재실행 불필요 — Tier 1에서 사전 계산 완료
8. **ret_df_is 첫 행을 0.0으로 설정** (research.md §4.2.4)
9. **`(ret_df_is == 0).sum() <= 10` 필터 적용** (research.md §4.2.7)
10. CAGR 계산 → 상위 `min(top_factors, len(valid))` 선정 → meta 생성
11. `calculate_downside_correlation()` → negative_corr
12. `find_optimal_mix()` + `simulate_constrained_weights(mode="simulation")`

Tier 3 (OOS 수익률, 매월):
13. `precomputed_ret_df.loc[oos_date, selected_factors]` → 팩터별 OOS 수익률 조회
14. portfolio_return = sum(weight[f] * oos_factor_return[f])
15. ew_return = mean(oos_factor_return[f])
    ⚠️ aggregate 재실행 불필요. 조회만 (밀리초).

**aggregate_factor_returns 호출 방식: A-1 확정**

```python
# A-1: pipeline 인스턴스의 메서드로 호출
# Tier 1에서 1회 실행: pipeline.aggregate_factor_returns(full_filtered, kept_abbrs, ...)
# Tier 2/3에서는 호출하지 않음 — precomputed_ret_df를 슬라이스/조회만.
```

#### 3.4.4 OOS 팩터 수익률 계산 (사전 계산 조회 방식)

OOS 팩터 수익률은 Tier 3에서 매월 **precomputed_ret_df를 조회**하여 얻는다.
별도의 compute_oos_factor_returns 함수가 필요하지 않을 수 있다.

```
OOS 수익률 계산 (Tier 3, 매월):
  oos_factor_returns = precomputed_ret_df.loc[oos_date, selected_factors]
  oos_return = (oos_factor_returns * weights).sum()
  oos_ew_return = oos_factor_returns.mean()
```

**기존 비용 로직과의 일관성 보장**:
  precomputed_ret_df는 Tier 1에서 aggregate_factor_returns를 전기간 데이터로
  1회 실행하여 생성했다. calculate_vectorized_return 내부의 
  cumulative_growth_block, drift, turnover, 30bp 비용이 시계열 전체에
  일관되게 적용되었으므로, OOS 월의 수익률도 기존 파이프라인과 동일하다.
  
  월별로 떼어 독립 계산하는 이전 방식의 비용 불일치 문제가 완전히 해결됨.

**Factor-level 거래비용 원칙 (§0.5 재확인)**:
  ret_df의 수익률은 이미 팩터 내부 종목 리밸런싱 비용(30bp)이 차감된 net-of-cost.
  팩터 가중치 변경에 대한 추가 거래비용은 적용하지 않는다.

#### 3.4.5 result_stitcher.py

```python
class WalkForwardResult:
    """
    Walk-Forward 결과를 담는 컨테이너.
    
    속성:
      기본 성과:
        - oos_returns: pd.Series (Date → monthly return)
        - oos_cumulative: pd.Series (누적 수익률)
        - oos_ew_returns: pd.Series (Date → OOS 동일가중 월간 수익률)
        - oos_ew_cumulative: pd.Series (OOS 동일가중 누적 수익률)
      
      가중치 이력:
        - weight_history: pd.DataFrame (Date × Factor, 각 월의 팩터 가중치)
      
      과적합 진단용 원자료 (Step 2에서 소비):
        - is_meta_history: list[pd.DataFrame]
          각 Tier 2 리밸런싱 시점의 IS meta (팩터별 CAGR 순위 포함).
          rank correlation 계산 + 팩터 선정 안정성(Jaccard) 계산에 사용.
          Jaccard는 meta['factorAbbreviation']에서 선정 팩터 셋을 추출하여 비교.
        - oos_factor_returns_history: list[dict]
          각 OOS 월의 팩터별 수익률 {factor_abbr: return}.
          rank correlation에서 OOS 순위를 매기는 데 사용.
        - rule_bundle_history: list[dict]
          각 Tier 1 리밸런싱 시점의 rule_bundle.
          규칙 안정성(rule stability) 보조 지표로 사용. Jaccard의 주 대상이 아님.
      
      리밸런싱 로그:
        - rebalance_log: pd.DataFrame
          (date, is_rule_rebal, is_weight_rebal, style_cap_relaxed 등)
    
    메서드:
      - calc_performance() → dict (CAGR, MDD, Sharpe, Calmar)
      - calc_ew_performance() → dict (OOS EW의 CAGR, MDD, Sharpe, Calmar)
      - compare_mp_vs_ew_oos() → dict (OOS 구간에서 MP vs. EW 비교)
      - plot_cumulative() → matplotlib Figure (MP, EW 두 곡선 겹쳐 그림)
      - to_csv(path) → 결과 저장
    """
```

### 3.5 실행 시간 예상

```
70개월 데이터, min_is=36, OOS=34개월 기준:

Tier 1 (6개월마다): 34/6 ≈ 6회
  × ([2]~[3] 규칙 학습 ~15초 + aggregate_factor_returns 1회 ~60초) = ~7.5분
  ※ aggregate를 Tier 1에서 전기간 1회 실행하므로 Tier 2/3에서 재실행 불필요
Tier 2 (3개월마다): 34/3 ≈ 12회
  × (ret_df 슬라이스 + CAGR 계산 + [5]~[6] ~25초) = ~5분
  ※ aggregate 재실행 없음, ret_df 슬라이스는 수초
Tier 3 (매월): 34회
  × (precomputed_ret_df에서 OOS 행 조회 ~0.1초) = ~3초
  ※ 기존 ~25분에서 수초로 대폭 개선

총 예상: ~12~15분 (기존 25~40분에서 대폭 개선)

⚠️ 15분 초과 시:
  - --num-sims를 100,000으로 줄여 MC 시간 단축 (Tier 2)
  - Tier 1 주기를 12개월로 늘려 aggregate 재실행 횟수 감소
```

### 3.6 주의사항

- **[2] batch lag(shift(1))와 Expanding Window의 호환성**: Expanding Window에서는 IS 시작점이 항상 전체 데이터의 첫 달로 고정되어 있으므로, shift(1)로 인한 NaN은 항상 '전체 데이터의 가장 첫 달'에만 발생한다. IS 범위를 뒤로 확장해도 추가적인 데이터 유실은 없다. 따라서 `slice_data_by_date`로 end_date만 잘라서 순수 함수에 넘겨도 안전하다. 단, 향후 Rolling Window(고정 구간) 옵션을 추가할 때는 is_start_date보다 1개월 앞선 데이터를 buffer로 포함시켜야 shift(1) 유실을 방지해야 한다.
- **MC 시뮬레이션의 random_seed**: 재현성을 위해 각 루프에서 동일한 seed를 사용하거나, 루프 인덱스를 seed로 사용하여 결정적(deterministic) 결과를 보장한다. 예: `random_seed = base_seed + loop_index` 방식으로 PIPELINE_PARAMS의 `random_seed`를 기반으로 파생.
- **[4]에서 상위 50개 선정**: IS 기간이 짧을수록 선정 불안정성이 높아진다. min_is_months=36이면 처음 IS는 36개월인데, `(ret_df == 0).sum() <= 10` 필터와 결합하면 유효 팩터가 50개 미만일 수 있다. `top_factors` 파라미터를 도입하되, 실제 유효 팩터 수가 `top_factors`보다 적으면 유효 팩터 전부를 사용하도록 `min(top_factors, len(valid_factors))`로 처리할 것.
- **최소 유효 팩터 수 방어**: 유효 팩터가 극단적으로 줄어들면(예: 5개 미만) 2-팩터 믹스 그리드 서치나 MC 시뮬레이션 자체가 무의미해진다. `MIN_REQUIRED_FACTORS = 5` (또는 PIPELINE_PARAMS로 설정)를 두고, 이 수 미만이면 해당 Tier 2 리밸런싱을 스킵하고 이전 가중치를 유지하며, rebalance_log에 "insufficient factors" 경고를 기록할 것.
- **MC 시뮬레이션 feasibility 실패 대비**: IS 기간이 짧고 팩터 수가 적으면, 스타일 캡 25% 제약을 만족하는 포트폴리오가 100만 개 중 하나도 없을 수 있다 (`ValueError("No feasible portfolios found")`). 기존 파이프라인의 test_mode에서 `style_cap=1.0`으로 완화하는 로직이 있으므로(research.md §4.3.4), Walk-Forward 루프에서도 feasibility 실패 시 style_cap을 단계적으로 완화하는 fallback을 구현할 것:
  ```
  시도 1: style_cap = 0.25 (기본)
  시도 2: style_cap = 0.40 (완화)
  시도 3: style_cap = 1.00 (무제약)
  → fallback 발생 시 rebalance_log에 경고 기록
  ```

---

## 4. Step 2: 과적합 진단 모듈

### 4.1 목적

Walk-Forward 결과를 분석하여 파이프라인의 과적합 정도를 정량화한다.

### 4.2 파일 구조

```
service/backtest/overfit_diagnostics.py   (신규)
```

### 4.3 입력/출력

- **입력**: `WalkForwardResult` (Step 1의 산출물)
- **출력**: 과적합 진단 리포트 dict + 콘솔 출력

### 4.4 진단 지표

**지표 우선순위**: OOS 기간이 34개월(3년 미만)으로 짧아 특정 매크로 레짐에 편향될 수 있다. 따라서 단일 숫자인 Deflation Ratio보다 **IS-OOS Rank Correlation(팩터 순위 유지력)을 1순위 지표로 신뢰**하고, Deflation Ratio와 Jaccard는 보조로 참고한다.

```
지표 우선순위:
  1순위: IS-OOS Rank Correlation (§4.4.3) — 팩터 예측력의 가장 신뢰할 수 있는 측정
  2순위: 팩터 선정 안정성 Jaccard (§4.4.2) — 모델 안정성 측정
  3순위: Deflation Ratio (§4.4.1) — 참고용, 단독 판단 금지
```

#### 4.4.1 Deflation Ratio (보조 지표)

```python
def calc_deflation_ratio(walk_forward_result, full_period_cagr):
    """
    IS 전체 기간 CAGR 대비 OOS 연환산 수익률의 비율.
    
    full_period_cagr: 전체 70개월을 IS로 사용하여 simulation 모드로 산출한 CAGR.
                      ⚠️ hardcoded 모드의 CAGR이 아님 (§0.6 참조).
    oos_cagr: Walk-Forward OOS 구간의 연환산 수익률
    
    deflation_ratio = oos_cagr / full_period_cagr
    
    ⚠️ 엣지케이스 처리:
      - full_period_cagr == 0: ratio = NaN, "IS 성과 없음" 경고
      - full_period_cagr < 0:
        ratio 해석이 반전됨. IS가 음수이고 OOS도 음수이면 ratio > 0이 되지만
        이는 "둘 다 손실"이므로 양호가 아님.
        → full_period_cagr < 0일 때는 ratio 대신 
          "IS CAGR: X%, OOS CAGR: Y%"를 직접 보고하고 ratio 해석을 스킵할 것.
      - oos_cagr < 0 and full_period_cagr > 0: ratio < 0 → "OOS에서 손실 전환"
    
    해석 (참고용, 단독 판단 금지, full_period_cagr > 0일 때만 유효):
      > 0.6: 양호 (IS 성과의 60% 이상 유지)
      0.3~0.6: 주의 (상당한 과적합)
      < 0.3: 심각 (IS 성과의 70% 이상 증발)
    
    ⚠️ 주의: OOS 기간이 34개월로 짧아 특정 매크로 환경(예: 하락장, 박스권)에 
    갇혀 있을 수 있다. Deflation Ratio가 비정상적으로 높거나(>1.0) 낮게 나올 수 있으므로
    이 수치 단독으로 판단하지 말고, IS-OOS Rank Correlation과 함께 해석할 것.
    """
```

#### 4.4.2 팩터 선정 안정성

```python
def calc_factor_selection_stability(is_meta_history):
    """
    **Tier 2** 리밸런싱마다 선정된 팩터 목록의 안정성 측정.
    
    ⚠️ 대상 주의:
      현재 Tier 1은 [2]~[3] 규칙 학습만 수행하고, 팩터 선정([4])은 Tier 2에서 한다.
      과적합 진단의 핵심은 "어떤 팩터가 선정되었는가"의 안정성이므로,
      Jaccard는 Tier 2의 cached_selected_factors (또는 is_meta_history의 팩터 셋)으로
      계산해야 한다.
      
      rule_bundle_history로 Jaccard를 계산하면 규칙 안정성(rule stability)은 볼 수 있지만
      선정 안정성(selection stability)은 측정하지 못한다.
      규칙 안정성은 별도 보조 지표로 분리한다.
    
    is_meta_history: list[pd.DataFrame]
      각 Tier 2 리밸런싱 시점의 meta DataFrame.
      meta['factorAbbreviation']에서 선정된 팩터 목록을 추출.
    
    Jaccard 유사도: 연속된 두 리밸런싱의 팩터 집합 교집합/합집합
    
    최소 샘플 요건:
      Jaccard를 계산하려면 최소 2번의 Tier 2 리밸런싱이 필요하다 (1개의 쌍).
      34개월 OOS, 3개월 주기이면 약 12회 리밸런싱 → 11개의 Jaccard 쌍.
      리밸런싱 횟수가 2회 미만이면 Jaccard = NaN, "샘플 부족" 경고를 출력할 것.
    
    해석 (70개월/50팩터 기준 — 자연적 변동이 크므로 임계값 완화):
      Jaccard > 0.6: 안정적 (팩터 대부분 유지)
      0.4~0.6: 보통 (acceptable)
      < 0.4: 불안정 (매번 다른 팩터 선정 → 노이즈 학습 의심)
      ⚠️ 데이터가 20년+로 확장되면 임계값을 0.7/0.5/0.3으로 상향 조정
    """
```

#### 4.4.3 IS-OOS 순위 상관 (1순위 지표)

```python
def calc_is_oos_rank_correlation(walk_forward_result):
    """
    각 Tier 2 리밸런싱 시점에서:
      - IS 기간의 팩터별 CAGR 순위 (파이프라인의 meta['cagr']에서 제공, research.md §2.2 [4])
      - 해당 OOS 기간의 팩터별 실현 수익률 순위 (동일한 팩터 세트에서 계산)
    를 비교하여 Spearman 순위 상관 계산.
    
    IS 순위 출처: _evaluate_universe에서 산출되는 meta DataFrame의 cagr 컬럼.
    OOS 순위 출처: OOS 기간 중 해당 팩터들의 실현 월간 수익률 합계 (또는 기하평균).
    두 순위 모두 동일한 팩터 세트(Tier 1 선정 팩터)에 대해 계산해야 한다.
    
    이것이 1순위 지표인 이유:
      Deflation Ratio는 OOS 기간의 매크로 환경에 좌우되지만,
      Rank Correlation은 "IS에서 좋았던 팩터가 OOS에서도 상대적으로 좋은가"를
      측정하므로, 환경 편향에 덜 민감하다.
    
    해석:
      양의 상관 (>0.3): IS에서 좋은 팩터가 OOS에서도 대체로 좋음 → 예측력 있음
      0 근처: IS 순위와 OOS 순위 무관 → 팩터 선정이 무의미
      음의 상관: IS에서 좋을수록 OOS에서 나쁨 → 명확한 과적합
    """
```

#### 4.4.4 종합 리포트

```python
def generate_overfit_report(walk_forward_result, full_period_cagr) -> dict:
    """
    위 3개 지표 + 추가 정보를 종합한 리포트.
    
    출력 항목 (우선순위 순):
      - is_oos_rank_spearman (값, p-value) ← 1순위 지표
      - avg_factor_jaccard ← 2순위 지표
      - deflation_ratio ← 3순위 (보조, 단독 판단 금지)
      - oos_cagr, oos_mdd, oos_sharpe
      - oos_ew_cagr, oos_ew_mdd, oos_ew_sharpe (OOS 동일가중 — 진짜 벤치마크)
      - mp_vs_ew_excess_cagr (OOS 구간에서 MP - EW)
      - mp_vs_ew_win_rate (OOS 구간에서 MP가 EW를 이긴 월 비율)
      - 해석 텍스트 (한국어, 아래 규칙 준수)
    
    해석 텍스트 작성 규칙:
      - 객관적 지표 수치와 임계값 기반 판정만 포함 (예: "Rank Corr = 0.42 > 0.3 → 양의 상관")
      - 주관적 평가나 투자 조언을 포함하지 않음
      - 경고/주의는 데이터 한계에 대한 사실만 기술
    
    해석 텍스트에 반드시 포함할 경고:
      "OOS 기간이 {n}개월로 3년 미만이므로 특정 매크로 환경에 편향되었을 수 있음.
       Deflation Ratio 수치 자체보다 IS-OOS Rank Correlation(팩터 순위 유지력)을
       더 신뢰할 것."
    
    한계점 명시 (리포트 하단에 포함):
      "본 백테스트는 factor-level에서 수행되었으며, 팩터 내부 종목 리밸런싱 비용(30bp)만 
       반영됨. 팩터 간 비중 변경(inter-factor rebalancing)에 따른 자산 배분 차원의 
       거래비용은 미반영이므로, OOS 수익률에 약간의 상방 편향이 존재할 수 있음."
    
    콘솔 출력: Rich 테이블 (기존 파이프라인의 Rich 스타일과 통일)
    파일 출력: output/overfit_diagnostics.csv
    """
```

---

## 5. main.py 확장

### 5.1 신규 커맨드

```python
# 기존 커맨드 유지
# python main.py download <start> <end>
# python main.py mp <start> <end>

# 신규 커맨드
# python main.py backtest <start> <end> [옵션]
# python main.py backtest test <file> [옵션]   ← test 모드 (소량 CSV, 통합 테스트용)

# 옵션:
#   --min-is-months        최소 IS 기간 (기본: 36)
#   --factor-rebal-months  Tier 1 리밸런싱 주기 (기본: 6)
#   --weight-rebal-months  Tier 2 리밸런싱 주기 (기본: 3)
#   --top-factors          상위 팩터 수 (기본: 50, 기존 파이프라인과 동일)
#   --num-sims             MC 시뮬레이션 횟수 (기본: 1_000_000, 속도 위해 줄일 수 있음)
#   --turnover-alpha       EMA 가중치 블렌딩 비율 (기본: 1.0 = 스무딩 없음)
#                          과적합 진단 시 1.0, 운용 시뮬레이션 시 0.5 권장
```

### 5.2 기존 mp 커맨드 확장

```python
# 기존 mp 커맨드에 벤치마크 비교 옵션 추가
# python main.py mp <start> <end> --benchmark
# → 파이프라인 완료 후 동일가중 벤치마크 비교 리포트 출력
```

---

## 6. 테스트 계획

### 6.1 단위 테스트

```
tests/
  ├── test_benchmark_comparison.py
  │     ├── test_equal_weight_return_calculation
  │     ├── test_cagr_formula_consistency  (파이프라인과 동일 공식)
  │     ├── test_ttest_with_known_data
  │     └── test_zero_excess_return_case
  │
  ├── test_data_slicer.py
  │     ├── test_slice_excludes_future_data
  │     ├── test_slice_returns_copy_not_view     (.copy() 확인)
  │     ├── test_oos_dates_start_after_min_is
  │     └── test_edge_case_insufficient_data
  │
  ├── test_walk_forward_engine.py
  │     ├── test_tier1_rebal_frequency           (rule_bundle이 6개월마다 갱신되는지)
  │     ├── test_tier2_rebal_frequency           ([4]~[6]이 3개월마다 재실행되는지)
  │     ├── test_tier2_slices_precomputed_retdf   (Tier 2가 precomputed_ret_df를 IS 구간으로 슬라이스)
  │     ├── test_tier2_uses_current_is_data      (Tier 2가 precomputed_ret_df의 IS 슬라이스를 사용)
  │     ├── test_oos_return_no_lookahead         (OOS 데이터가 IS 학습에 미포함)
  │     ├── test_precomputed_ret_matches_full    (precomputed ret_df가 전체 기간 1회 실행과 동일)
  │     ├── test_tier3_uses_precomputed_lookup   (Tier 3이 aggregate 재실행 없이 조회만 하는지)
  │     ├── test_rule_bundle_applied_to_oos      (IS 규칙이 OOS에 올바르게 적용되는지)
  │     ├── test_rule_bundle_no_filtered_data    (rule_bundle에 DataFrame이 포함되지 않는지)
  │     ├── test_no_double_transaction_cost      (팩터 가중치 변경 시 추가 30bp 없는지)
  │     ├── test_oos_ew_return_same_universe     (EW와 MP가 동일 팩터 유니버스 사용)
  │     ├── test_date_slice_inclusive            (slice가 <= end_date로 동작하는지)
  │     ├── test_cache_persistence_between_rebals
  │     ├── test_simulation_mode_only            (hardcoded 모드 미사용 확인)
  │     └── test_result_stitching
  │
  └── test_overfit_diagnostics.py
        ├── test_deflation_ratio_calculation
        ├── test_deflation_ratio_negative_cagr    (음수 CAGR 엣지케이스)
        ├── test_jaccard_uses_tier2_selection     (Tier 2 is_meta_history 기반, Tier 1 아님)
        ├── test_jaccard_min_samples              (Tier 2 리밸런싱 2회 미만 시 NaN)
        ├── test_rank_correlation_uses_same_factors (IS/OOS 동일 팩터셋 순위 비교)
        └── test_interpretation_objectivity        (주관적 평가 미포함)
```

### 6.2 통합 테스트

- 기존 `test_mode`용 소량 CSV 데이터로 Walk-Forward 전체 루프 실행
- OOS 수익률이 IS 수익률의 단순 복사가 아닌지 검증 (look-ahead 없음 확인)
- 기존 `python main.py mp test <file>` 결과와 `python main.py backtest test <file>` 결과 비교

### 6.3 핵심 검증 항목

```
✅ 날짜 슬라이싱이 <= (inclusive)로 구현되어 있는가?
   → slice_data_by_date가 <= end_date로 필터 (< 아님, off-by-one 방지)
   → is_end_date = all_dates[min_is_months + i - 1], OOS = all_dates[min_is_months + i]

✅ OOS 구간의 데이터가 IS 학습에 사용되지 않는가?
   → .copy()가 적용되어 원본 raw_data가 오염되지 않는지 확인

✅ [1] 데이터 로딩 시 _prepare_metadata까지 호출되는가?
   → M_RETURN merge가 완료된 raw_data를 사용하는지 확인

✅ Tier 1이 전기간 팩터 수익률을 사전 계산하는가?
   → rule_bundle 학습 후 전체 데이터에 규칙 적용 → aggregate_factor_returns 1회 실행
   → precomputed_ret_df가 전체 월을 포함하는지

✅ Tier 2가 precomputed_ret_df를 IS 구간으로 슬라이스하는가?
   → aggregate 재실행 없이 precomputed_ret_df[index <= is_end_date]만
   → 슬라이스된 ret_df_is에 첫 행 0.0 + (==0) 필터 적용

✅ fit/transform 분리가 올바른가?
   → OOS에서 IS의 rule_bundle 규칙이 적용(transform)되는지
   → OOS에서 규칙을 다시 학습(fit)하지 않는지

✅ OOS 팩터 수익률이 precomputed_ret_df 조회로 얻어지는가?
   → Tier 3에서 aggregate_factor_returns 재실행 없이 .loc[oos_date] 조회만
   → precomputed_ret_df는 Tier 1에서 전기간 1회 계산한 것
   → drift/turnover/30bp 비용이 시계열 전체에 일관되게 반영됨

✅ 거래비용 이중 적용이 없는가?
   → factor return 내부의 30bp만 적용 (calculate_vectorized_return)
   → 팩터 가중치 변경에 대한 추가 30bp 없음

✅ OOS 동일가중 수익률이 MP와 동일한 팩터 유니버스를 사용하는가?
   → Tier 2에서 선정된 팩터 목록이 EW와 MP에 동일하게 적용되는지

✅ aggregate_factor_returns를 A-1(pipeline 인스턴스 메서드)로 호출하는가?
   → 로직 복제(A-2)나 코드 리팩터링(A-3)을 사용하지 않는지

✅ simulation 모드만 사용되는가?
   → Step 0과 Walk-Forward 모두 hardcoded 모드가 섞이지 않는지

✅ Jaccard가 Tier 2의 선정 팩터(is_meta_history)를 기준으로 계산되는가?
   → Tier 1의 rule_bundle_history가 아닌 Tier 2의 meta['factorAbbreviation']

✅ CAGR 공식이 기존 파이프라인과 동일한가?
   → months = len(ret_df) - 1, ret_df 첫 행 = 0.0 기준 일치

✅ random_seed가 루프 간 결정적인가?
   → 동일 데이터로 2회 실행 시 동일 결과

✅ WalkForwardResult에 진단 원자료가 모두 포함되는가?
   → is_meta_history (rank corr + Jaccard용)
   → oos_factor_returns_history (rank corr OOS 순위용)
   → rule_bundle_history (규칙 안정성 보조 지표용)
```

---

## 7. 디렉토리 구조 (최종)

```
bok/
  ├── main.py                          (수정: backtest 커맨드 + --benchmark 옵션 추가)
  ├── config.py                        (수정 없음)
  ├── research.md                      (수정 없음)
  ├── service/
  │   ├── pipeline/
  │   │   ├── model_portfolio.py       (수정 없음)
  │   │   ├── factor_analysis.py       (수정 없음)
  │   │   ├── correlation.py           (수정 없음)
  │   │   ├── optimization.py          (수정 없음)
  │   │   ├── weight_construction.py   (수정 없음)
  │   │   ├── pipeline_utils.py        (수정 없음)
  │   │   └── benchmark_comparison.py  (신규 - Step 0)
  │   └── backtest/                    (신규 디렉토리)
  │       ├── __init__.py
  │       ├── walk_forward_engine.py   (신규 - Step 1)
  │       ├── data_slicer.py           (신규 - Step 1)
  │       ├── result_stitcher.py       (신규 - Step 1)
  │       └── overfit_diagnostics.py   (신규 - Step 2)
  └── tests/
      ├── test_benchmark_comparison.py (신규)
      ├── test_data_slicer.py          (신규)
      ├── test_walk_forward_engine.py  (신규)
      └── test_overfit_diagnostics.py  (신규)
```

---

## 8. 구현 순서 체크리스트

```
Phase 1: Step 0 (벤치마크 비교)
  □ benchmark_comparison.py 작성
  □ main.py에 --benchmark 옵션 추가
  □ test_benchmark_comparison.py 작성 및 통과
  □ 기존 데이터로 실행하여 MP vs. 동일가중 결과 확인

Phase 2: Step 1 (Walk-Forward 엔진)
  □ data_slicer.py 작성
  □ test_data_slicer.py 작성 및 통과
  □ walk_forward_engine.py 작성 (계층적 리밸런싱)
  □ result_stitcher.py 작성
  □ test_walk_forward_engine.py 작성 및 통과
  □ main.py에 backtest 커맨드 추가
  □ 기존 데이터로 전체 Walk-Forward 실행 (10~15분 이내 확인)

Phase 3: Step 2 (과적합 진단)
  □ overfit_diagnostics.py 작성
  □ test_overfit_diagnostics.py 작성 및 통과
  □ Walk-Forward 결과로 진단 리포트 생성
  □ 결과 해석 및 파이프라인 개선 방향 도출
```

---

## 9. 성공 기준

### 9.1 엔지니어링 성공 기준 (pass/fail — 코드가 맞는지)

1. **Look-ahead 없음**: OOS 구간의 데이터가 IS 학습에 절대 사용되지 않는다.
2. **결정적 재현성**: 동일 입력 + 동일 seed로 2회 실행 시 동일 결과.
3. **기존 코드 무결성**: `python main.py mp` 커맨드의 결과가 변경 전후로 동일.
4. **유니버스 일치**: OOS MP와 OOS EW가 동일한 팩터 유니버스(Tier 2 선정 팩터)를 사용.
5. **CAGR 공식 일관성**: 모든 CAGR 계산이 `months = len(ret_df) - 1` 기준.
6. **실행 시간**: backtest 커맨드 전체가 15분 이내에 완료 (precompute 방식 적용 기준).
7. **모드 통일**: IS 기준값과 OOS 모두 simulation 모드. hardcoded 혼입 없음.

### 9.2 전략 분석 목표 (코드 정확성과 무관 — 전략은 질 수도 있음)

아래는 분석하고 싶은 질문이지, 코드의 pass/fail 기준이 아니다.
코드가 완전히 맞아도 전략이 동일가중을 이기지 못할 수 있으며, 그것 자체가 유의미한 분석 결과이다.

- Step 0: IS 구간에서 MP CAGR ≥ EW CAGR인가?
- Step 1: OOS에서 MP가 EW를 이기는가?
- Step 2: IS-OOS Rank Correlation이 양의 상관인가? Jaccard > 0.5인가?
