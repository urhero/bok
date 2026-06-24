# -*- coding: utf-8 -*-
"""백테스트 선정/스무딩 경로의 프로세스 간 재현성(determinism) 계약 테스트.

배경: 팩터 선정/가중치 스무딩 경로가 Python ``set`` 반복에 의존하면,
PYTHONHASHSEED 가 프로세스마다 문자열 해시를 랜덤화하므로 출력(팩터 컬럼
순서, 합산 순서로 인한 float 말단자릿수, 타이 경계 선정 집합)이 실행마다
달라진다. 그 결과 ``walk_forward_results.csv`` / ``walk_forward_weight_history.csv``
가 동일 코드인데도 byte-identical 하지 않아 회귀 비교가 불가능해진다.

이 테스트는 동일 입력을 서로 다른 PYTHONHASHSEED 하위 프로세스에서 실행해
출력이 byte-identical 한지 검증한다. set 반복 순서에 의존하는 코드가 다시
들어오면 (seed 별 출력이 갈라져) 실패한다.

단일 프로세스 안에서는 해시 시드가 고정이라 비결정성이 드러나지 않으므로
반드시 별도 프로세스 + 서로 다른 PYTHONHASHSEED 로 검증해야 한다.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# 하위 프로세스에서 실행할 프로브: 선정/스무딩 경로를 고정 입력으로 호출하고
# 결과를 표준 출력에 직렬화한다. set 반복 순서에 의존하면 seed 별로 달라진다.
_PROBE = r"""
import pandas as pd
from service.pipeline.smoothing import step_smooth
from service.backtest.factor_selection import apply_selection_hysteresis

# 1) step_smooth: union = set(target)|set(prev) 반복 순서가 반환 dict 키 순서와
#    내부 float 합산 순서(=값 말단자릿수)에 영향. 14개 팩터로 set 순서 변동을 노출.
target = {("FA%02d" % i): 1.0 / 12 for i in range(12)}
prev = {("FA%02d" % i): w for i, w in enumerate([0.06] * 8 + [0.18, 0.18, 0.0, 0.0])}
prev["FZ01"] = 0.10   # target 에 없는 탈락 팩터
prev["FZ02"] = 0.10
res = step_smooth(target, prev, step=0.01, deadband=0.003, months=1)
print("STEP|" + "|".join("%s=%r" % (k, v) for k, v in res.items()))

# 2) apply_selection_hysteresis: 동점(AA, BB 모두 2.0)인 exit 의 정렬 타이브레이크가
#    prev_selected(set) 반복 순서에 의존하면 살아남는 팩터가 seed 별로 달라진다.
scores = pd.Series({"A": 5.0, "B": 3.0, "AA": 2.0, "BB": 2.0, "C": 1.0})
out = apply_selection_hysteresis(["A", "B", "C"], scores, {"A", "B", "AA", "BB"}, margin=0.25)
print("HYS|" + ",".join(out))
"""


def _run_probe(seed: str) -> str:
    """주어진 PYTHONHASHSEED 로 프로브를 실행하고 표준 출력을 반환."""
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = seed
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    proc = subprocess.run(
        [sys.executable, "-c", _PROBE],
        capture_output=True, text=True, env=env, cwd=str(PROJECT_ROOT),
    )
    assert proc.returncode == 0, f"probe(seed={seed}) failed:\n{proc.stderr}"
    return proc.stdout


_SEEDS = ["0", "1", "2", "3", "7"]


@pytest.fixture(scope="module")
def probe_outputs() -> list[str]:
    return [_run_probe(s) for s in _SEEDS]


def _line(output: str, prefix: str) -> str:
    for ln in output.splitlines():
        if ln.startswith(prefix):
            return ln
    raise AssertionError(f"prefix {prefix!r} not found in probe output:\n{output}")


def test_step_smooth_deterministic_across_hash_seeds(probe_outputs):
    """step_smooth 의 반환 키 순서/값이 PYTHONHASHSEED 와 무관해야 한다."""
    step_lines = {_line(o, "STEP|") for o in probe_outputs}
    assert len(step_lines) == 1, (
        "step_smooth 출력이 hash seed 별로 다름 (set 반복 의존):\n"
        + "\n".join(sorted(step_lines))
    )


def test_selection_hysteresis_deterministic_across_hash_seeds(probe_outputs):
    """동점 경계에서도 선정 팩터 집합/순서가 hash seed 와 무관해야 한다."""
    hys_lines = {_line(o, "HYS|") for o in probe_outputs}
    assert len(hys_lines) == 1, (
        "apply_selection_hysteresis 출력이 hash seed 별로 다름 (set 타이브레이크 의존):\n"
        + "\n".join(sorted(hys_lines))
    )


def test_full_probe_byte_identical_across_hash_seeds(probe_outputs):
    """프로브 전체 표준 출력이 seed 와 무관하게 완전히 동일해야 한다."""
    assert len(set(probe_outputs)) == 1, "선정/스무딩 경로 출력이 프로세스마다 다름"
