# -*- coding: utf-8 -*-
"""MP 배포 배수 (2026-08-19)."""
import pandas as pd
import pytest

from service.pipeline.weight_construction import apply_multiplier, resolve_multiplier


@pytest.fixture
def hist(tmp_path):
    p = tmp_path / "mp_multiplier.csv"
    p.write_text(
        "effective_date,multiplier,note\n"
        "2025-06-30,0.45,initial\n"
        "2026-03-31,0.60,raised\n",
        encoding="utf-8",
    )
    return p


def test_step_function_holds_until_next_change(hist):
    """입력값은 다음 변경 전까지 유효 (계단식)."""
    assert resolve_multiplier("2025-06-30", hist) == 0.45
    assert resolve_multiplier("2025-12-31", hist) == 0.45
    assert resolve_multiplier("2026-03-30", hist) == 0.45
    assert resolve_multiplier("2026-03-31", hist) == 0.60
    assert resolve_multiplier("2026-06-30", hist) == 0.60


def test_before_first_entry_is_unscaled(hist):
    """첫 유효일 이전은 미적용(1.0) — 과거 산출물 회귀 보존."""
    assert resolve_multiplier("2025-05-31", hist) == 1.0


def test_missing_file_is_unscaled(tmp_path):
    assert resolve_multiplier("2026-06-30", tmp_path / "none.csv") == 1.0


def test_apply_scales_weight_cols_only():
    """factor_weight 는 피벗 컬럼 키이자 팩터 배분이라 스케일하지 않는다."""
    df = pd.DataFrame({
        "mp_ls_weight": [0.4, -0.4], "ls_weight": [0.4, -0.4],
        "style_ls_weight": [0.4, -0.4], "factor_weight": [1.0, 1.0],
    })
    out = apply_multiplier(df.copy(), 0.45)
    assert out["mp_ls_weight"].tolist() == pytest.approx([0.18, -0.18])
    assert out["ls_weight"].tolist() == pytest.approx([0.18, -0.18])
    assert out["style_ls_weight"].tolist() == pytest.approx([0.18, -0.18])
    assert out["factor_weight"].tolist() == [1.0, 1.0]


def test_apply_identity_returns_unchanged():
    df = pd.DataFrame({"mp_ls_weight": [0.4, -0.4]})
    pd.testing.assert_frame_equal(apply_multiplier(df.copy(), 1.0), df)


def test_neutrality_preserved():
    """배수는 롱/숏에 동일 적용 — 달러 중립 유지."""
    df = pd.DataFrame({"mp_ls_weight": [0.431, -0.431]})
    out = apply_multiplier(df, 0.45)
    assert out["mp_ls_weight"].sum() == pytest.approx(0.0)
    assert out.loc[out.mp_ls_weight > 0, "mp_ls_weight"].sum() == pytest.approx(0.431 * 0.45)


def test_target_gross_normalization():
    """목표 gross 정규화: netting 이 달라도 노출은 항상 목표값."""
    from service.pipeline.weight_construction import multiplier_for_target
    # 2025-06 실측 gross 0.8980 / 2026-06 0.8621 -> 둘 다 0.40 으로 착지
    for gross in (0.8980, 0.8621, 1.20):
        m = multiplier_for_target(gross, 0.40)
        assert gross * m == pytest.approx(0.40)


def test_target_gross_zero_book_falls_back():
    from service.pipeline.weight_construction import multiplier_for_target
    assert multiplier_for_target(0.0, 0.40) == 1.0
