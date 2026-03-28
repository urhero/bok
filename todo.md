# TODO — Awesome-Cohen (BoK) 미완료 작업

> 작성일: 2026-03-28
> 이 문서만 보고 나중에 독립적으로 실행 가능하도록 상세하게 기술합니다.

---

## 1. [CRITICAL] DB 비밀번호 git 히스토리 제거

### 배경
- commit `b319225` (초기 커밋)의 `config.py`에 DB 비밀번호 `REDACTED`가 평문으로 포함되어 있었음
- 현재 `config.py`는 `.env`에서 로드하도록 수정 완료되었으나, git 히스토리에 이전 버전이 남아있음
- `git show b319225:config.py` 로 누구나 복원 가능

### 작업 순서

#### Step 1: DB 비밀번호 변경 (DBA 작업)
```sql
-- SQL Server Management Studio에서 실행
ALTER LOGIN sa WITH PASSWORD = '새_비밀번호';
```
- 서버: `10.206.1.19:9433`
- 변경 후 `.env` 파일의 `USER_PWD` 값도 업데이트

#### Step 2: git filter-repo로 히스토리 재작성
```bash
# 1. git-filter-repo 설치
pip install git-filter-repo

# 2. 백업 생성 (필수!)
git clone --mirror https://github.com/urhero/bok.git bok-backup.git

# 3. 히스토리에서 비밀번호 제거
#    방법 A: config.py 파일 전체를 히스토리에서 제거
git filter-repo --path config.py --invert-paths

#    방법 B: 특정 문자열만 치환 (config.py 히스토리는 유지)
git filter-repo --replace-text <(echo 'REDACTED==>REDACTED')

# 4. force push (주의: 모든 협업자가 re-clone 필요)
git remote add origin https://github.com/urhero/bok.git
git push --force --all
git push --force --tags
```

#### Step 3: 협업자 알림
- 모든 클론한 사람에게 re-clone 요청
- 이전 clone의 reflog에도 비밀번호가 남아있으므로 삭제 권장

### 검증
```bash
# 히스토리에 비밀번호가 남아있지 않은지 확인
git log --all -p | grep "***REMOVED***"  # 결과가 없어야 함
```

---

## 2. [CRITICAL] 읽기 전용 DB 서비스 계정 생성

### 배경
- 현재 `sa` (시스템 관리자) 계정으로 DB에 접속 중
- 이 파이프라인은 `SELECT`만 수행하므로 최소 권한 원칙 위반
- `sa` 계정이 탈취되면 DB 전체(삭제, 수정 포함)가 위험

### 작업 순서

#### Step 1: DBA에게 계정 생성 요청
```sql
-- SQL Server에서 실행 (DBA 권한 필요)

-- 1. 로그인 생성
CREATE LOGIN bok_reader WITH PASSWORD = '강력한_비밀번호';

-- 2. GLOBAL 데이터베이스에 사용자 매핑
USE GLOBAL;
CREATE USER bok_reader FOR LOGIN bok_reader;

-- 3. SELECT 권한만 부여
GRANT SELECT ON [dbo].[clarifi_mxcn1a_afl] TO bok_reader;
GRANT SELECT ON [dbo].[clarifi_mxwo_afl] TO bok_reader;  -- 향후 사용 가능
```

#### Step 2: .env 업데이트
```bash
# .env 파일 수정
USER_NAME=bok_reader
USER_PWD=강력한_비밀번호
```

#### Step 3: factor_query.py allowlist 확인
- `ALLOWED_UNIVERSES`에 새 계정이 접근 가능한 테이블만 포함되어 있는지 확인
- 현재: `{"clarifi_mxcn1a_afl", "clarifi_mxwo_afl"}`

### 검증
```bash
# download 명령으로 DB 연결 테스트
python main.py download 2026-01-31 2026-02-28 --incremental
```

---

## 3. [SUGGESTION] pre-commit hook + detect-secrets 설치

### 배경
- TODO #1의 비밀번호 유출은 pre-commit hook이 있었으면 방지 가능했음
- 향후 `.env` 값이 코드에 실수로 포함되는 것을 원천 차단

### 작업
```bash
# 1. pre-commit 설치
pip install pre-commit

# 2. .pre-commit-config.yaml 생성
cat > .pre-commit-config.yaml << 'EOF'
repos:
  - repo: https://github.com/Yelp/detect-secrets
    rev: v1.4.0
    hooks:
      - id: detect-secrets
        args: ['--baseline', '.secrets.baseline']
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: check-added-large-files
        args: ['--maxkb=100000']  # 100MB (parquet 파일 허용)
      - id: check-merge-conflict
      - id: trailing-whitespace
EOF

# 3. baseline 생성 (기존 파일의 false positive 제거)
detect-secrets scan > .secrets.baseline

# 4. hook 활성화
pre-commit install
```

### 검증
```bash
# 의도적으로 비밀번호를 포함한 커밋 시도 → 차단되어야 함
echo "PASSWORD=test123" >> temp.py
git add temp.py
git commit -m "test"  # detect-secrets가 차단해야 함
rm temp.py
```

---

## 4. [SUGGESTION] assert를 ValueError로 변환

### 배경
- `pipeline_utils.py:73`의 `assert len(factor_data_list) == len(factor_abbr_list)`
- `python -O` (최적화 모드)에서는 assert가 무시됨
- 금융 데이터 파이프라인에서는 방어적 프로그래밍이 필요

### 작업
```python
# pipeline_utils.py:73 변경
# Before:
assert len(factor_data_list) == len(factor_abbr_list), ...

# After:
if len(factor_data_list) != len(factor_abbr_list):
    raise ValueError(
        f"factor_data_list ({len(factor_data_list)}) and "
        f"factor_abbr_list ({len(factor_abbr_list)}) length mismatch"
    )
```

---

## 5. [SUGGESTION] _evaluate_universe 빈 DataFrame guard

### 배경
- `model_portfolio.py:259`에서 `ret_df.loc[ret_df.index[0]] = 0.0`
- `aggregate_factor_returns`가 빈 DataFrame을 반환하면 `IndexError` 발생
- 에러 메시지가 불명확하여 디버깅 어려움

### 작업
```python
# model_portfolio.py _evaluate_universe 메서드, ret_df 생성 직후에 추가:
ret_df = aggregate_factor_returns(filtered_data, kept_abbrs)
if ret_df.empty:
    raise ValueError(
        f"No valid factor returns after aggregation. "
        f"Input: {len(filtered_data)} factors, {len(kept_abbrs)} abbreviations"
    )
ret_df.loc[ret_df.index[0]] = 0.0
```

---

## 6. [SUGGESTION] ValueError 메시지에 컨텍스트 추가

### 배경
- `optimization.py:246`의 `ValueError("No feasible portfolios found")`
- 디버깅 시 어떤 파라미터로 실패했는지 알 수 없음

### 작업
```python
# optimization.py:246 변경
raise ValueError(
    f"No feasible portfolios found after {num_sims} simulations. "
    f"K={K}, styles={S}, style_cap={style_cap}, "
    f"valid_count={sum(len(c) for c in all_cagrs) if all_cagrs else 0}"
)
```

---

## 7. [SUGGESTION] PIPELINE_PARAMS를 실제 코드에 적용

### 배경
- `config.py`에 `PIPELINE_PARAMS` 딕셔너리를 추가했으나, 실제 코드에서는 아직 참조하지 않음
- 현재는 매직넘버가 코드에 그대로 존재하고, PIPELINE_PARAMS는 문서화 역할만 수행
- 점진적으로 코드의 매직넘버를 PIPELINE_PARAMS 참조로 교체해야 함

### 적용 대상 (우선순위순)

| 파라미터 | 현재 위치 | 코드 내 값 |
|---------|----------|-----------|
| `style_cap` | `optimization.py:131` | `0.25` |
| `transaction_cost_bps` | `weight_construction.py:58` | `30.0` |
| `top_factor_count` | `model_portfolio.py:285` | `50` |
| `spread_threshold_pct` | `factor_analysis.py:180` | `0.10` |
| `backtest_start` | `weight_construction.py:45` | `"2017-12-31"` |
| `sub_factor_rank_weights` | `optimization.py:56` | `(0.7, 0.3)` |
| `portfolio_rank_weights` | `optimization.py:99,255` | `(0.6, 0.4)` |
| `min_sector_stocks` | `factor_analysis.py:89` | `10` |
| `max_zero_return_months` | `model_portfolio.py:266` | `10` |

### 주의사항
- 각 함수의 시그니처에 파라미터를 추가하거나, Pipeline 클래스에서 config를 주입하는 방식으로 적용
- **hardcoded_weights.csv 관련 값은 절대 변경하지 말 것** (프로덕션 가중치)
- 변경 시 반드시 기존 테스트 통과 확인 + test_data.csv 결과 비교

---

## 완료된 작업 (참고)

이전 세션에서 완료한 수정사항 목록:

### 세션 1 (2026-03-28 초반)
- C1: `report_generator.py` 깨진 임포트 수정
- C2: `factor_analysis.py` division by zero 방어 (`np.where`)
- I1: `optimization.py` 연환산 분모 `12/(T-1)` 통일
- I2: `correlation.py` Bessel's correction 추가
- C3: `model_portfolio.py` `np.sign()**2` → 명시적 boolean mask
- I7: `parquet_io.py` docstring 기본값 수정

### 세션 2 (2026-03-28 중반)
- `model_portfolio.py` end_date Timestamp 변환
- `pipeline_utils.py` dropna 팩터 드롭 로깅 + assert
- `model_portfolio.py` weight_frames 빈 경우 guard
- `model_portfolio.py` sys.exit(0) 제거
- `factor_analysis.py` L/S 라벨 0개 경고

### 세션 3 (2026-03-28 후반)
- `config.py` sa/IP 기본값 제거 + PIPELINE_PARAMS 추가
- `factor_query.py` universe allowlist 검증
- `model_portfolio.py` path traversal 검증
- `optimization.py` random_seed + default_rng
- `model_portfolio.py` validation.py 함수 파이프라인 통합
- `correlation.py` Bessel 컬럼별 유효 카운트 보정
- `report_generator.py` zero-prepend + 필터 추가
- 테스트 50개 신규 작성 (총 140개)
- README.md + research.md 업데이트
