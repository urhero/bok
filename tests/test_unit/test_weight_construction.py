# -*- coding: utf-8 -*-
"""construct_long_short_df 및 calculate_vectorized_return 함수 유닛 테스트.

롱/숏 분리, 동일가중 비중 부여, 포트폴리오 수익률 계산을 검증한다.
"""
from __future__ import annotations

import numpy as np
import numpy as np
import pandas as pd
import pytest

from service.pipeline.weight_construction import construct_long_short_df, calculate_vectorized_return
from service.factor.factor_returns import aggregate_factor_returns


# ═══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def basic_labeled_data() -> pd.DataFrame:
    """기본 라벨링 데이터: 2개 날짜, 롱/숏/뉴트럴 혼합."""
    return pd.DataFrame({
        "ddt": pd.to_datetime([
            "2024-01-31", "2024-01-31", "2024-01-31", "2024-01-31",
            "2024-02-29", "2024-02-29", "2024-02-29", "2024-02-29",
        ]),
        "gvkeyiid": ["A", "B", "C", "D", "A", "B", "C", "D"],
        "ticker": ["T_A", "T_B", "T_C", "T_D", "T_A", "T_B", "T_C", "T_D"],
        "M_RETURN": [0.03, -0.01, 0.02, -0.02, 0.01, 0.04, -0.03, 0.00],
        "label": [1, -1, 0, 1, 1, -1, 0, 1],
    })


@pytest.fixture
def labeled_data_with_early_dates() -> pd.DataFrame:
    """2017-12-31 이전 데이터를 포함하는 라벨링 데이터."""
    return pd.DataFrame({
        "ddt": pd.to_datetime([
            "2017-11-30", "2017-11-30",  # 제외 대상
            "2017-12-31", "2017-12-31",  # 경계값 (포함)
            "2018-01-31", "2018-01-31",  # 포함
        ]),
        "gvkeyiid": ["A", "B", "A", "B", "A", "B"],
        "ticker": ["T_A", "T_B", "T_A", "T_B", "T_A", "T_B"],
        "M_RETURN": [0.05, -0.03, 0.02, -0.01, 0.04, -0.02],
        "label": [1, -1, 1, -1, 1, -1],
    })


@pytest.fixture
def single_date_portfolio() -> pd.DataFrame:
    """단일 날짜 포트폴리오 데이터 (calculate_vectorized_return용)."""
    return pd.DataFrame({
        "ddt": pd.to_datetime(["2024-01-31", "2024-01-31"]),
        "gvkeyiid": ["A", "B"],
        "ticker": ["T_A", "T_B"],
        "M_RETURN": [0.03, 0.01],
        "label": [1, 1],
        "signal": ["L", "L"],
        "num": [2, 2],
        "return_weight": [0.5, 0.5],
        "turnover_weight": [0.5, 0.5],
    })


@pytest.fixture
def multi_date_portfolio() -> pd.DataFrame:
    """다중 날짜 포트폴리오 데이터 (calculate_vectorized_return용)."""
    return pd.DataFrame({
        "ddt": pd.to_datetime([
            "2024-01-31", "2024-01-31",
            "2024-02-29", "2024-02-29",
            "2024-03-31", "2024-03-31",
        ]),
        "gvkeyiid": ["A", "B", "A", "B", "A", "B"],
        "ticker": ["T_A", "T_B", "T_A", "T_B", "T_A", "T_B"],
        "M_RETURN": [0.03, 0.01, 0.02, -0.01, 0.04, 0.02],
        "label": [1, 1, 1, 1, 1, 1],
        "signal": ["L", "L", "L", "L", "L", "L"],
        "num": [2, 2, 2, 2, 2, 2],
        "return_weight": [0.5, 0.5, 0.5, 0.5, 0.5, 0.5],
        "turnover_weight": [0.5, 0.5, 0.5, 0.5, 0.5, 0.5],
    })


# ═══════════════════════════════════════════════════════════════════════════════
# construct_long_short_df Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestConstructLongShortDfBasic:
    """기본 롱/숏 분리 테스트."""

    def test_returns_two_dataframes(self, basic_labeled_data):
        """(long_df, short_df) 2-튜플을 반환하는지 확인."""
        result = construct_long_short_df(basic_labeled_data)
        assert len(result) == 2
        long_df, short_df = result
        assert isinstance(long_df, pd.DataFrame)
        assert isinstance(short_df, pd.DataFrame)

    def test_long_df_contains_only_label_1(self, basic_labeled_data):
        """long_df에는 label=1인 종목만 포함되는지 확인."""
        long_df, _ = construct_long_short_df(basic_labeled_data)
        assert (long_df["label"] == 1).all()

    def test_short_df_contains_only_label_neg1(self, basic_labeled_data):
        """short_df에는 label=-1인 종목만 포함되는지 확인."""
        _, short_df = construct_long_short_df(basic_labeled_data)
        assert (short_df["label"] == -1).all()

    def test_neutral_excluded(self, basic_labeled_data):
        """label=0(중립)인 종목은 롱/숏 모두에서 제외되는지 확인."""
        long_df, short_df = construct_long_short_df(basic_labeled_data)
        all_tickers = pd.concat([long_df["ticker"], short_df["ticker"]])
        # C는 label=0이므로 제외
        assert "T_C" not in all_tickers.values


class TestConstructLongShortDfSignal:
    """signal 컬럼 테스트."""

    def test_long_signal_is_l(self, basic_labeled_data):
        """long_df의 signal 컬럼이 'L'인지 확인."""
        long_df, _ = construct_long_short_df(basic_labeled_data)
        assert (long_df["signal"] == "L").all()

    def test_short_signal_is_s(self, basic_labeled_data):
        """short_df의 signal 컬럼이 'S'인지 확인."""
        _, short_df = construct_long_short_df(basic_labeled_data)
        assert (short_df["signal"] == "S").all()


class TestConstructLongShortDfWeights:
    """가중치 계산 테스트."""

    def test_return_weight_equals_label_over_count(self, basic_labeled_data):
        """return_weight = label / 같은 날짜·시그널 내 종목 수."""
        long_df, _ = construct_long_short_df(basic_labeled_data)
        # 2024-01-31: A(L), D(L) → 2개, return_weight = 1/2 = 0.5
        jan_long = long_df[long_df["ddt"] == pd.Timestamp("2024-01-31")]
        assert len(jan_long) == 2
        np.testing.assert_almost_equal(jan_long["return_weight"].iloc[0], 0.5)

    def test_turnover_weight_is_abs_return_weight(self, basic_labeled_data):
        """turnover_weight = abs(return_weight)."""
        long_df, short_df = construct_long_short_df(basic_labeled_data)
        for df in [long_df, short_df]:
            if not df.empty:
                np.testing.assert_array_almost_equal(
                    df["turnover_weight"].values,
                    np.abs(df["return_weight"].values),
                )

    def test_short_return_weight_is_negative(self, basic_labeled_data):
        """short_df의 return_weight는 음수 (label=-1이므로 -1/count)."""
        _, short_df = construct_long_short_df(basic_labeled_data)
        assert (short_df["return_weight"] < 0).all()


class TestConstructLongShortDfDateFilter:
    """날짜 필터링 테스트."""

    def test_dates_before_cutoff_excluded(self, labeled_data_with_early_dates):
        """2017-12-31 이전 데이터가 제외되는지 확인."""
        long_df, short_df = construct_long_short_df(labeled_data_with_early_dates)
        all_dates = pd.concat([long_df["ddt"], short_df["ddt"]])
        assert (all_dates >= pd.Timestamp("2017-12-31")).all()

    def test_cutoff_boundary_included(self, labeled_data_with_early_dates):
        """2017-12-31 경계값은 포함되는지 확인."""
        long_df, short_df = construct_long_short_df(labeled_data_with_early_dates)
        all_dates = pd.concat([long_df["ddt"], short_df["ddt"]])
        assert pd.Timestamp("2017-12-31") in all_dates.values


class TestConstructLongShortDfRequiredColumns:
    """필수 컬럼 존재 테스트."""

    def test_output_columns(self, basic_labeled_data):
        """출력에 필수 컬럼이 존재하는지 확인."""
        long_df, short_df = construct_long_short_df(basic_labeled_data)
        expected_cols = {"signal", "num", "return_weight", "turnover_weight"}
        for df in [long_df, short_df]:
            if not df.empty:
                assert expected_cols.issubset(set(df.columns))


# ═══════════════════════════════════════════════════════════════════════════════
# calculate_vectorized_return Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestCalculateVectorizedReturnBasic:
    """기본 수익률 계산 테스트."""

    def test_returns_three_dataframes(self, multi_date_portfolio):
        """(gross, net, cost) 3-튜플을 반환하는지 확인."""
        result = calculate_vectorized_return(multi_date_portfolio, "TestFactor")
        assert len(result) == 3
        gross, net, cost = result
        assert isinstance(gross, pd.DataFrame)
        assert isinstance(net, pd.DataFrame)
        assert isinstance(cost, pd.DataFrame)

    def test_column_name_matches_factor_abbr(self, multi_date_portfolio):
        """컬럼명이 factor_abbr와 일치하는지 확인."""
        gross, net, cost = calculate_vectorized_return(multi_date_portfolio, "SalesAcc")
        assert "SalesAcc" in gross.columns
        assert "SalesAcc" in net.columns
        assert "SalesAcc" in cost.columns

    def test_empty_portfolio_returns_empty_not_keyerror(self):
        """빈 포트폴리오(롱-only/숏-only 팩터의 빈 쪽)는 KeyError 대신 빈 결과.

        construct_long_short_df 가 한쪽 종목 0개를 반환하면 pivot 에 return_weight
        컬럼이 없어 과거 KeyError 로 전체 mp 가 죽었다(2026-06 EPSEstDispFY1C).
        """
        empty = pd.DataFrame(columns=["ddt", "gvkeyiid", "M_RETURN", "return_weight", "turnover_weight"])
        gross, net, cost = calculate_vectorized_return(empty, "F1")
        assert list(gross.columns) == ["F1"] and gross.empty
        assert net.empty and cost.empty

    def test_first_row_is_zero(self, multi_date_portfolio):
        """첫 번째 행의 gross return이 0인지 확인."""
        gross, _, _ = calculate_vectorized_return(multi_date_portfolio, "TestFactor")
        assert gross.iloc[0, 0] == 0.0


class TestCalculateVectorizedReturnRelationships:
    """수익률 관계 테스트."""

    def test_net_equals_gross_minus_cost(self, multi_date_portfolio):
        """net = gross - cost 관계가 성립하는지 확인."""
        gross, net, cost = calculate_vectorized_return(multi_date_portfolio, "TestFactor")
        expected_net = gross.values - cost.values
        np.testing.assert_array_almost_equal(net.values, expected_net, decimal=10)

    def test_cost_is_non_negative(self, multi_date_portfolio):
        """거래비용이 음수가 아닌지 확인."""
        _, _, cost = calculate_vectorized_return(multi_date_portfolio, "TestFactor")
        assert (cost.values >= -1e-10).all()

    def test_entry_exit_trades_are_costed(self):
        """편입 매수/편출 매도도 턴오버 비용에 포함된다 (2026-07 비용 과소계상 수정).

        수익률 0으로 고정해 drift 를 제거하면 1월 턴오버는
        |0.5-0.5|(A 유지) + |0-0.5|(B 편출 매도) + |0.5-0|(C 편입 매수) = 1.0.
        """
        df = pd.DataFrame({
            "ddt": pd.to_datetime(["2024-01-31", "2024-01-31", "2024-02-29", "2024-02-29"]),
            "gvkeyiid": ["A", "B", "A", "C"],  # 2월에 B 편출, C 편입
            "M_RETURN": [0.0, 0.0, 0.0, 0.0],
            "return_weight": [0.5, 0.5, 0.5, 0.5],
            "turnover_weight": [0.5, 0.5, 0.5, 0.5],
        })
        _, _, cost = calculate_vectorized_return(df, "F", cost_bps=30.0)
        assert cost.iloc[0, 0] == pytest.approx(30.0 / 1e4 * 1.0)
        # 마지막 월은 다음 목표 비중이 없음(청산 아님) -> 비용 0
        assert cost.iloc[1, 0] == 0.0

    def test_custom_cost_bps(self, multi_date_portfolio):
        """cost_bps=0이면 gross == net인지 확인."""
        gross, net, cost = calculate_vectorized_return(
            multi_date_portfolio, "TestFactor", cost_bps=0.0
        )
        np.testing.assert_array_almost_equal(gross.values, net.values, decimal=10)


class TestCalculateVectorizedReturnShape:
    """출력 형태 테스트."""

    def test_output_is_single_column(self, multi_date_portfolio):
        """각 출력이 단일 컬럼 DataFrame인지 확인."""
        gross, net, cost = calculate_vectorized_return(multi_date_portfolio, "TestFactor")
        assert gross.shape[1] == 1
        assert net.shape[1] == 1
        assert cost.shape[1] == 1

    def test_output_rows_match_dates(self, multi_date_portfolio):
        """출력 행 수가 고유 날짜 수와 일치하는지 확인."""
        gross, _, _ = calculate_vectorized_return(multi_date_portfolio, "TestFactor")
        n_dates = multi_date_portfolio["ddt"].nunique()
        assert gross.shape[0] == n_dates


# ---------------------------------------------------------------------------
# aggregate_factor_returns 병렬화 byte-identity
# ---------------------------------------------------------------------------
class TestAggregateFactorReturnsParallel:
    def test_parallel_matches_serial_byte_identical(self, basic_labeled_data):
        """병렬(n_jobs=-1)과 직렬(n_jobs=1) 출력이 bit 단위로 동일해야 한다.

        _PARALLEL_MIN_FACTORS(8) 초과하도록 10개 팩터로 복제해 병렬 경로를 강제한다.
        assert_frame_equal 은 값 + 컬럼 순서를 모두 검사 -> 순서 보존(byte-identity 핵심)까지 증명.
        """
        data = [basic_labeled_data.copy() for _ in range(10)]
        abbrs = [f"F{i}" for i in range(10)]

        serial = aggregate_factor_returns(data, abbrs, backtest_start="2017-12-31", n_jobs=1)
        parallel = aggregate_factor_returns(data, abbrs, backtest_start="2017-12-31", n_jobs=-1)

        assert not parallel.empty, "병렬 경로가 실제로 결과를 내야 한다 (빈 프레임 동치 방지)"
        pd.testing.assert_frame_equal(serial, parallel, check_exact=True)


# ── apply_sector_short_cap (2026-07-30 채택) ──────────────────────────────

def _mk_agg(rows):
    return pd.DataFrame(rows, columns=["ddt", "ticker", "isin", "gvkeyiid", "sec", "mp_ls_weight", "factor_weight"])


def test_sector_short_cap_reduces_over_sector_and_preserves_gross():
    from service.pipeline.weight_construction import apply_sector_short_cap
    d = pd.Timestamp("2026-06-30")
    agg = _mk_agg([
        [d, "A", "iA", "gA", "Financials", -0.30, 0.1],
        [d, "B", "iB", "gB", "Energy", -0.10, 0.1],
        [d, "C", "iC", "gC", "Tech", 0.40, 0.1],
    ])
    out = apply_sector_short_cap(agg, cap=0.5)   # Financials 숏 비중 0.75 > 0.5
    shorts = out[out["mp_ls_weight"] < 0]
    total_sg = shorts["mp_ls_weight"].abs().sum()
    fin = shorts[shorts["sec"] == "Financials"]["mp_ls_weight"].abs().sum()
    assert abs(total_sg - 0.40) < 1e-9, "총 숏 gross 보존"
    assert fin <= 0.5 * total_sg + 1e-9, "초과 섹터가 캡 이하로"
    assert out[out["mp_ls_weight"] > 0]["mp_ls_weight"].sum() == 0.40, "롱 사이드 불변"


def test_sector_short_cap_noop_when_under_cap_or_off():
    from service.pipeline.weight_construction import apply_sector_short_cap
    d = pd.Timestamp("2026-06-30")
    agg = _mk_agg([
        [d, "A", "iA", "gA", "Financials", -0.20, 0.1],
        [d, "B", "iB", "gB", "Energy", -0.20, 0.1],
    ])
    assert apply_sector_short_cap(agg, cap=0.6).equals(agg)
    assert apply_sector_short_cap(agg, cap=None).equals(agg)


def test_sector_short_cap_matches_old_one_pass_when_no_reviolation():
    """재위반 없는 케이스의 구 1-pass 수식 재현 (2섹터 케이스).

    주의: 일반 케이스에서는 total/free_g 합산 단위(종목단 vs 섹터단) 차이로
    ULP(~1e-16) 수준 차이가 가능 (Opus 검증 2026-08-25). 이 테스트는 두 합이
    일치하는 최소 케이스에서 수식 구조가 같음을 고정하는 용도."""
    from service.pipeline.weight_construction import apply_sector_short_cap
    d = pd.Timestamp("2026-06-30")
    agg = _mk_agg([
        [d, "A", "iA", "gA", "Financials", -0.30, 0.1],
        [d, "B", "iB", "gB", "Energy", -0.10, 0.1],
        [d, "C", "iC", "gC", "Tech", 0.40, 0.1],
    ])
    out = apply_sector_short_cap(agg, cap=0.5)
    # 구 로직 float 연산 순서 그대로 재현 (0.30-0.20 의 이진 오차까지 동일해야 byte 보존)
    freed = 0.30 - 0.5 * 0.40
    assert out.loc[0, "mp_ls_weight"] == -0.30 * ((0.5 * 0.40) / 0.30)
    assert out.loc[1, "mp_ls_weight"] == -0.10 * (1.0 + freed / 0.10)
    assert out.loc[2, "mp_ls_weight"] == 0.40


def test_sector_short_cap_waterfills_reviolation():
    """재분배 수혜 섹터가 캡을 재초과하면 수렴까지 반복해야 한다 (구 1-pass 버그)."""
    from service.pipeline.weight_construction import apply_sector_short_cap
    d = pd.Timestamp("2026-06-30")
    # cap 0.15, 숏 7개 섹터 (합 1.0): A 0.25 초과 -> 1-pass 재분배 시 B/C(0.14)가 0.158로 재초과
    secs = {"A": 0.25, "B": 0.14, "C": 0.14, "D": 0.13, "E": 0.12, "F": 0.11, "G": 0.11}
    rows = [[d, s, f"i{s}", f"g{s}", s, -g, 0.1] for s, g in secs.items()]
    rows.append([d, "L", "iL", "gL", "Tech", 1.0, 0.1])
    agg = _mk_agg(rows)
    out = apply_sector_short_cap(agg, cap=0.15)
    shorts = out[out["mp_ls_weight"] < 0]
    total_sg = shorts["mp_ls_weight"].abs().sum()
    sec_g = shorts.groupby("sec")["mp_ls_weight"].apply(lambda x: x.abs().sum())
    assert abs(total_sg - 1.0) < 1e-9, "feasible 케이스는 총 숏 gross 보존"
    assert (sec_g <= 0.15 * total_sg + 1e-9).all(), f"전 섹터 캡 준수 실패: {sec_g.to_dict()}"
    assert out[out["mp_ls_weight"] > 0]["mp_ls_weight"].sum() == 1.0, "feasible 케이스 롱 불변"


def test_sector_short_cap_infeasible_cap_wins_and_stays_neutral():
    """숏 섹터 < 1/cap 개면 캡 우선: 숏 gross 축소 + 롱 동반 축소 (달러 중립 유지)."""
    from service.pipeline.weight_construction import apply_sector_short_cap
    d = pd.Timestamp("2026-06-30")
    rows = [[d, s, f"i{s}", f"g{s}", s, -0.25, 0.1] for s in ["A", "B", "C", "D"]]
    rows.append([d, "L", "iL", "gL", "Tech", 1.0, 0.1])
    agg = _mk_agg(rows)
    out = apply_sector_short_cap(agg, cap=0.15)  # 4섹터 x 0.15 = 0.6 < 1.0 인피저블
    shorts = out[out["mp_ls_weight"] < 0]
    sec_g = shorts.groupby("sec")["mp_ls_weight"].apply(lambda x: x.abs().sum())
    assert (sec_g <= 0.15 * 1.0 + 1e-9).all(), "원 gross 기준 캡 준수"
    total_sg = shorts["mp_ls_weight"].abs().sum()
    assert abs(total_sg - 0.6) < 1e-9, "숏 gross 0.6 으로 축소"
    long_sum = out[out["mp_ls_weight"] > 0]["mp_ls_weight"].sum()
    assert abs(long_sum - total_sg) < 1e-9, "롱 동반 축소 -> 달러 중립"


def test_stock_level_sector_short_cap_shares_helper():
    """백테스트 경로(stock_weights_at)도 재위반 시 water-filling 수렴해야 한다 (parity).

    섹터별 종목 수 = weight_construction 재위반 시나리오와 동일 비율 (A 25 / B·C 14 /
    D 13 / E 12 / F·G 11): 구 1-pass 는 A 절단분 재분배로 B·C 가 0.1587 > cap 재초과
    -> 구 코드에서 실패하는 회귀 가드 (2026-08-25 Sonnet 검증 지적으로 강화).
    """
    from service.backtest.stock_level import stock_weights_at
    t = pd.Timestamp("2026-06-30")
    counts = {"A": 25, "B": 14, "C": 14, "D": 13, "E": 12, "F": 11, "G": 11}
    recs = [{"ddt": t, "gvkeyiid": f"s{s}{i}", "label": -1, "M_RETURN": 0.0, "sec": s}
            for s, n in counts.items() for i in range(n)]
    recs.append({"ddt": t, "gvkeyiid": "long1", "label": 1, "M_RETURN": 0.0, "sec": "Tech"})
    frames = {"F1": pd.DataFrame(recs)}
    w, _r = stock_weights_at(frames, {"F1": 1.0}, t, pd.Timestamp("2000-01-31"),
                             sector_short_cap=0.15)
    shorts = w[w < 0]
    total_sg = shorts.abs().sum()
    sec_g = shorts.abs().groupby(shorts.index.str[1]).sum()  # gvkeyiid 두번째 문자 = 섹터
    assert (sec_g <= 0.15 * total_sg + 1e-9).all(), f"캡 재초과 방치 (구 1-pass 버그): {sec_g.to_dict()}"
    assert abs(total_sg - 1.0) < 1e-9, "feasible(7섹터) -> 숏 gross 보존 (팩터 숏 사이드 = wf 1.0)"
