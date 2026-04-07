# -*- coding: utf-8 -*-
"""Walk-Forward 결과 컨테이너 및 성과 계산.

OOS 월별 결과를 접합하여 누적 수익률, 성과 지표, 가중치 이력 등을 제공한다.
Funnel Value-Add Test를 위해 EW_All, EW_Top50 수익률도 관리한다.
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class WalkForwardResult:
    """Walk-Forward 결과를 담는 컨테이너.

    Attributes:
        oos_returns: OOS 월간 MP 수익률 Series.
        oos_ew_returns: OOS 월간 동일가중 수익률 Series (선정 팩터).
        oos_ew_all_returns: OOS 월간 전체 유효 팩터 동일가중 수익률.
        oos_ew_top50_returns: OOS 월간 Top-50 후보군 동일가중 수익률.
        oos_cumulative: OOS MP 누적 수익률.
        oos_ew_cumulative: OOS EW 누적 수익률.
        weight_history: 팩터 가중치 이력 DataFrame.
        is_meta_history: Tier 2 리밸런싱 시점별 IS meta 리스트.
        active_factors_history: Tier 2 시점별 weight>0 팩터 set 리스트.
        oos_all_factor_returns_history: OOS 월별 전체 팩터 수익률 dict 리스트.
        rebalance_log: 리밸런싱 로그 DataFrame.
    """

    def __init__(self, results: list[dict[str, Any]]):
        self._raw_results = results

        if not results:
            self.oos_returns = pd.Series(dtype=float)
            self.oos_ew_returns = pd.Series(dtype=float)
            self.oos_ew_all_returns = pd.Series(dtype=float)
            self.oos_ew_top50_returns = pd.Series(dtype=float)
            self.oos_cumulative = pd.Series(dtype=float)
            self.oos_ew_cumulative = pd.Series(dtype=float)
            self.oos_ew_all_cumulative = pd.Series(dtype=float)
            self.oos_ew_top50_cumulative = pd.Series(dtype=float)
            self.weight_history = pd.DataFrame()
            self.is_meta_history = []
            self.oos_factor_returns_history = []
            self.active_factors_history = []
            self.oos_all_factor_returns_history = []
            self.rebalance_log = pd.DataFrame()
            self.is_full_period_cagr = 0.0
            return

        dates = [r["date"] for r in results]
        mp_rets = [r["oos_return"] for r in results]
        ew_rets = [r["oos_ew_return"] for r in results]

        self.oos_returns = pd.Series(mp_rets, index=dates, name="oos_return")
        self.oos_ew_returns = pd.Series(ew_rets, index=dates, name="oos_ew_return")
        self.oos_cumulative = (1 + self.oos_returns).cumprod()
        self.oos_ew_cumulative = (1 + self.oos_ew_returns).cumprod()

        # EW_All: 전체 유효 팩터 동일가중 수익률
        ew_all_rets = []
        for r in results:
            all_fr = r.get("oos_all_factor_returns", {})
            val = np.nanmean(list(all_fr.values())) if all_fr else 0.0
            ew_all_rets.append(0.0 if np.isnan(val) else val)
        self.oos_ew_all_returns = pd.Series(ew_all_rets, index=dates, name="oos_ew_all_return")
        self.oos_ew_all_cumulative = (1 + self.oos_ew_all_returns).cumprod()

        # EW_Top50: Top-50 후보군 동일가중 수익률
        ew_top50_rets = []
        for r in results:
            all_fr = r.get("oos_all_factor_returns", {})
            top50 = r.get("top50_factors", [])
            if all_fr and top50:
                top50_vals = [all_fr[f] for f in top50 if f in all_fr]
                val50 = np.nanmean(top50_vals) if top50_vals else 0.0
                ew_top50_rets.append(0.0 if np.isnan(val50) else val50)
            else:
                ew_top50_rets.append(0.0)
        self.oos_ew_top50_returns = pd.Series(ew_top50_rets, index=dates, name="oos_ew_top50_return")
        self.oos_ew_top50_cumulative = (1 + self.oos_ew_top50_returns).cumprod()

        # 가중치 이력
        weight_records = []
        for r in results:
            if r.get("weights"):
                row = {"date": r["date"]}
                row.update(r["weights"])
                weight_records.append(row)
        self.weight_history = pd.DataFrame(weight_records).set_index("date") if weight_records else pd.DataFrame()

        # IS meta 이력 (Tier 2 리밸런싱 시점만)
        self.is_meta_history = [r["is_meta"] for r in results if r.get("is_weight_rebal") and r.get("is_meta") is not None]

        # Active factors 이력 (Tier 2 리밸런싱 시점만, weight>0 팩터 set)
        self.active_factors_history = [
            set(r.get("active_factors", []))
            for r in results if r.get("is_weight_rebal")
        ]

        # OOS 팩터 수익률 이력
        self.oos_factor_returns_history = [r.get("oos_factor_returns", {}) for r in results]

        # 전체 팩터 수익률 이력 (Percentile Tracking용)
        self.oos_all_factor_returns_history = [r.get("oos_all_factor_returns", {}) for r in results]

        # IS 전체 기간 MP CAGR (Deflation Ratio용 - 마지막 Tier 2 시점 기준)
        is_cagrs = [r.get("is_mp_cagr", 0.0) for r in results if r.get("is_weight_rebal") and r.get("is_mp_cagr") is not None]
        self.is_full_period_cagr = is_cagrs[-1] if is_cagrs else 0.0

        # 리밸런싱 로그
        log_rows = [{
            "date": r["date"],
            "is_rule_rebal": r.get("is_rule_rebal", False),
            "is_weight_rebal": r.get("is_weight_rebal", False),
        } for r in results]
        self.rebalance_log = pd.DataFrame(log_rows).set_index("date")

    def calc_performance(self) -> dict[str, float]:
        """OOS MP 성과 지표를 계산한다."""
        return self._calc_perf(self.oos_returns, self.oos_cumulative)

    def calc_ew_performance(self) -> dict[str, float]:
        """OOS EW 성과 지표를 계산한다 (선정 팩터 동일가중)."""
        return self._calc_perf(self.oos_ew_returns, self.oos_ew_cumulative)

    def calc_ew_all_performance(self) -> dict[str, float]:
        """OOS EW_All 성과: 전체 유효 팩터 동일가중."""
        return self._calc_perf(self.oos_ew_all_returns, self.oos_ew_all_cumulative)

    def calc_ew_top50_performance(self) -> dict[str, float]:
        """OOS EW_Top50 성과: Top-50 후보군 동일가중."""
        return self._calc_perf(self.oos_ew_top50_returns, self.oos_ew_top50_cumulative)

    def compare_mp_vs_ew_oos(self) -> dict[str, Any]:
        """OOS 구간에서 MP vs. EW를 비교한다."""
        mp_perf = self.calc_performance()
        ew_perf = self.calc_ew_performance()
        excess = self.oos_returns - self.oos_ew_returns

        result = {
            "mp_cagr": mp_perf["cagr"],
            "ew_cagr": ew_perf["cagr"],
            "excess_cagr": mp_perf["cagr"] - ew_perf["cagr"],
            "mp_mdd": mp_perf["mdd"],
            "ew_mdd": ew_perf["mdd"],
            "mp_sharpe": mp_perf["sharpe"],
            "ew_sharpe": ew_perf["sharpe"],
            "win_rate": (excess > 0).mean() if len(excess) > 0 else 0.0,
        }
        return result

    def to_csv(self, path: str) -> None:
        """결과를 CSV로 저장한다."""
        df = pd.DataFrame({
            "date": self.oos_returns.index,
            "mp_return": self.oos_returns.values,
            "ew_return": self.oos_ew_returns.values,
            "ew_all_return": self.oos_ew_all_returns.values,
            "ew_top50_return": self.oos_ew_top50_returns.values,
            "mp_cumulative": self.oos_cumulative.values,
            "ew_cumulative": self.oos_ew_cumulative.values,
            "ew_all_cumulative": self.oos_ew_all_cumulative.values,
            "ew_top50_cumulative": self.oos_ew_top50_cumulative.values,
        })
        df.to_csv(path, index=False)
        logger.info("Walk-Forward results saved to %s", path)

    @staticmethod
    def _calc_perf(returns: pd.Series, cumulative: pd.Series) -> dict[str, float]:
        """월간 수익률로부터 CAGR, MDD, Sharpe, Calmar를 계산한다."""
        if len(returns) == 0:
            return {"cagr": 0.0, "mdd": 0.0, "sharpe": 0.0, "calmar": 0.0}

        months = len(returns)
        cagr = cumulative.iloc[-1] ** (12 / months) - 1 if months > 0 else 0.0

        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        mdd = drawdown.min()

        sharpe = (returns.mean() / returns.std() * np.sqrt(12)) if returns.std() > 0 else 0.0
        calmar = (cagr / abs(mdd)) if mdd != 0 else 0.0

        return {"cagr": cagr, "mdd": mdd, "sharpe": sharpe, "calmar": calmar}
