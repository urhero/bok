# TODO — BOK 미완료 작업

> 최종 업데이트: 2026-03-28
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
git log --all -p | grep "OLD_PASSWORD"  # 결과가 없어야 함
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

## 완료된 작업 (참고)

### 세션 1 — 초기 코드 리뷰 수정 (6건)
- [x] C1: `report_generator.py` 깨진 임포트 수정 (`evaluate_factor_universe` → `aggregate_factor_returns`)
- [x] C2: `factor_analysis.py` division by zero 방어 (`np.where(count > 1, ..., np.nan)`)
- [x] I1: `optimization.py` 연환산 분모 `12/T` → `12/(T-1)` 통일
- [x] I2: `correlation.py` Bessel's correction 추가
- [x] C3: `model_portfolio.py` `np.sign()**2` → 명시적 boolean mask
- [x] I7: `parquet_io.py` docstring 기본값 "5%" → "10%"

### 세션 2 — 브레인스토밍 합의 기반 수정 (6건)
- [x] `model_portfolio.py` end_date `pd.Timestamp` 변환 (타입 안정성)
- [x] `pipeline_utils.py` NaN 팩터 드롭 warning 로깅 추가
- [x] `model_portfolio.py` weight_frames 빈 경우 guard + early return
- [x] `model_portfolio.py` `sys.exit(0)` 제거 → early return
- [x] `factor_analysis.py` L/S 라벨 0개 경고 로그
- [x] `pipeline_utils.py` 리스트 길이 검증 추가

### 세션 3 — 다각도 리뷰 (보안/엔지니어링/코드리뷰) 수정 (8건)
- [x] `config.py` sa/IP 기본값 제거, `.env` 필수화
- [x] `factor_query.py` universe `ALLOWED_UNIVERSES` allowlist 검증
- [x] `model_portfolio.py` test_file path traversal 검증
- [x] `optimization.py` `random_seed` 파라미터 + `np.random.default_rng` 재현성
- [x] `model_portfolio.py` `validation.py` 함수 3개 파이프라인 통합 (`validate_return_matrix`, `validate_output_weights`, `validate_weights_sum_to_one`)
- [x] `correlation.py` Bessel correction 컬럼별 유효 관측 수 보정
- [x] `report_generator.py` zero-prepend + 불충분 팩터 필터 추가
- [x] 테스트 50개 신규 작성 (총 140개): `test_filter_and_label_factors.py`, `test_weight_construction.py`, `test_find_optimal_mix.py`

### 세션 4 — SUGGESTION 이슈 수정 (5건)
- [x] S1: `pipeline_utils.py` `assert` → `ValueError` 변환 (python -O 안전)
- [x] S2: `model_portfolio.py` `_evaluate_universe` 빈 DataFrame guard
- [x] S3: `optimization.py` ValueError 메시지에 컨텍스트 추가 (num_sims, K, S, style_cap)
- [x] S4: `PIPELINE_PARAMS` 9개 매직넘버 전체 코드 적용 (config → 함수 파라미터 전달)
- [x] S5: `.pre-commit-config.yaml` + `detect-secrets` + `.secrets.baseline` 설정

### 문서 업데이트
- [x] `README.md` — PIPELINE_PARAMS 테이블, 보안 설정 섹션, NaN 처리, L/S 검증, random_seed 추가
- [x] `research.md` — §4.1.3/4.1.4 PIPELINE_PARAMS 참조, §4.2.6 early return, §4.2.8 boolean mask, §4.3.1 div-by-zero guard, §4.4.2 Bessel, §4.4.4 early return, §4.4.5 random_seed, §4.4.6 allowlist, §4.4.7 PIPELINE_PARAMS, §4.4.8 pre-commit, CAGR 수식 주석
- [x] `todo.md` 작성 및 업데이트
- [x] `.env` + `.env.example` 생성
