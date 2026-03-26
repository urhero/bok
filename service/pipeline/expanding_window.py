# -*- coding: utf-8 -*-
"""Expanding Window 백테스트 엔진.

기존 팩터 파이프라인 + TFT를 expanding window 방식으로 실행하여
out-of-sample 성과를 측정한다. 4가지 모드(static, factor-timing, satellite, hybrid)를
지원하며, compare 모드로 전체 비교표를 생성할 수 있다.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("output/backtest")


# ─────────────────────────────────────────────────────────────────────────────
# 성과 측정 유틸리티
# ─────────────────────────────────────────────────────────────────────────────
def calculate_performance_metrics(returns_series: pd.Series) -> dict:
    """월간 수익률 시리즈 → 성과 지표.

    Args:
        returns_series: 월간 수익률 (index=날짜, values=수익률)

    Returns:
        dict with CAGR, Annual_Vol, Sharpe, MDD, Calmar, Hit_Rate, etc.
    """
    if returns_series.empty or len(returns_series) < 2:
        return {
            "CAGR": 0.0, "Annual_Vol": 0.0, "Sharpe": 0.0, "MDD": 0.0,
            "Calmar": 0.0, "Hit_Rate": 0.0, "Avg_Monthly_Return": 0.0,
            "Worst_Month": 0.0, "Best_Month": 0.0,
        }

    r = returns_series.values
    n_months = len(r)
    n_years = n_months / 12.0

    # 누적 수익률
    cum = np.cumprod(1 + r)
    total_return = cum[-1] / 1.0 - 1.0

    # CAGR
    if n_years > 0 and cum[-1] > 0:
        cagr = (cum[-1]) ** (1 / n_years) - 1
    else:
        cagr = 0.0

    # Annualized Volatility
    annual_vol = np.std(r, ddof=1) * np.sqrt(12) if len(r) > 1 else 0.0

    # Sharpe (rf=0)
    sharpe = cagr / annual_vol if annual_vol > 0 else 0.0

    # Maximum Drawdown
    running_max = np.maximum.accumulate(cum)
    drawdown = (cum / running_max) - 1
    mdd = drawdown.min()

    # Calmar Ratio
    calmar = cagr / abs(mdd) if abs(mdd) > 1e-10 else 0.0

    # Hit Rate
    hit_rate = np.mean(r > 0) if len(r) > 0 else 0.0

    return {
        "CAGR": round(cagr, 6),
        "Annual_Vol": round(annual_vol, 6),
        "Sharpe": round(sharpe, 4),
        "MDD": round(mdd, 6),
        "Calmar": round(calmar, 4),
        "Hit_Rate": round(hit_rate, 4),
        "Avg_Monthly_Return": round(np.mean(r), 6),
        "Worst_Month": round(np.min(r), 6),
        "Best_Month": round(np.max(r), 6),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 결과 데이터 클래스
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class BacktestResult:
    """백테스트 결과 저장."""
    mode: str
    monthly_returns: pd.DataFrame  # columns: date, core, satellite(optional), blended
    cumulative_returns: pd.DataFrame
    stats: dict
    monthly_weights: Dict[str, pd.DataFrame] = field(default_factory=dict)
    factor_weights_history: Dict[str, pd.DataFrame] = field(default_factory=dict)

    def summary(self) -> pd.DataFrame:
        """성과 요약표."""
        rows = []
        for key, val in self.stats.items():
            rows.append({"Metric": key, "Value": val})
        return pd.DataFrame(rows)

    def plot(self, save_path: Optional[str] = None) -> None:
        """누적 수익률 차트."""
        fig, ax = plt.subplots(figsize=(12, 6))
        cum = self.cumulative_returns

        if "blended" in cum.columns:
            ax.plot(cum.index, cum["blended"], label=f"{self.mode} (Blended)", linewidth=2)
        if "core" in cum.columns:
            ax.plot(cum.index, cum["core"], label="Core", linestyle="--", alpha=0.7)
        if "satellite" in cum.columns:
            ax.plot(cum.index, cum["satellite"], label="Satellite", linestyle=":", alpha=0.7)

        ax.set_title(f"Expanding Window Backtest — {self.mode}")
        ax.set_xlabel("Date")
        ax.set_ylabel("Cumulative Return")
        ax.legend()
        ax.grid(True, alpha=0.3)
        plt.tight_layout()

        if save_path:
            fig.savefig(save_path, dpi=150)
            logger.info("[BacktestResult] 차트 저장: %s", save_path)
        plt.close(fig)

    def save(self, output_dir: Optional[Path] = None) -> None:
        """결과를 CSV로 저장."""
        out = output_dir or OUTPUT_DIR
        out.mkdir(parents=True, exist_ok=True)

        self.monthly_returns.to_csv(out / f"backtest_{self.mode}.csv", index=False)
        self.summary().to_csv(out / f"summary_{self.mode}.csv", index=False)
        self.plot(save_path=str(out / f"cumulative_{self.mode}.png"))

        if self.factor_weights_history:
            rows = []
            for date, wdf in self.factor_weights_history.items():
                wdf_copy = wdf.copy()
                wdf_copy["date"] = date
                rows.append(wdf_copy)
            if rows:
                pd.concat(rows, ignore_index=True).to_csv(
                    out / f"factor_weights_history_{self.mode}.csv", index=False,
                )


@dataclass
class CompareResult:
    """compare 모드: 4가지 전략 비교."""
    results: Dict[str, BacktestResult]

    def comparison_table(self) -> pd.DataFrame:
        """4가지 모드 성과 비교표."""
        records = {}
        for mode_name, bt in self.results.items():
            records[mode_name] = bt.stats
        df = pd.DataFrame(records)
        df.index.name = "Metric"
        return df

    def plot_all(self, save_path: Optional[str] = None) -> None:
        """4가지 누적 수익률을 하나의 차트에 오버레이."""
        fig, ax = plt.subplots(figsize=(14, 7))

        colors = {"static": "#1f77b4", "factor-timing": "#ff7f0e",
                  "satellite": "#2ca02c", "hybrid": "#d62728"}

        for mode_name, bt in self.results.items():
            cum = bt.cumulative_returns
            col = "blended" if "blended" in cum.columns else cum.columns[0]
            ax.plot(
                cum.index, cum[col],
                label=mode_name, linewidth=2,
                color=colors.get(mode_name, None),
            )

        ax.set_title("Expanding Window Backtest — Strategy Comparison")
        ax.set_xlabel("Date")
        ax.set_ylabel("Cumulative Return")
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3)
        plt.tight_layout()

        if save_path:
            fig.savefig(save_path, dpi=150)
            logger.info("[CompareResult] 비교 차트 저장: %s", save_path)
        plt.close(fig)

    def save(self, output_dir: Optional[Path] = None) -> None:
        """전체 결과 저장."""
        out = output_dir or OUTPUT_DIR
        out.mkdir(parents=True, exist_ok=True)

        comp = self.comparison_table()
        comp.to_csv(out / "comparison_table.csv")
        self.plot_all(save_path=str(out / "cumulative_chart.png"))

        for mode_name, bt in self.results.items():
            bt.save(output_dir=out)


# ─────────────────────────────────────────────────────────────────────────────
# 수익률 계산 헬퍼
# ─────────────────────────────────────────────────────────────────────────────
def _calculate_portfolio_return(
    weights_df: pd.DataFrame,
    mreturn_df: pd.DataFrame,
    month_date: str,
    weight_col: str = "stock_weight",
    cost_bps: float = 30.0,
    prev_weights_df: Optional[pd.DataFrame] = None,
) -> float:
    """포트폴리오 비중 + 실현 수익률 → 해당 월 포트폴리오 수익률.

    Args:
        weights_df: DataFrame with gvkeyiid, {weight_col}
        mreturn_df: DataFrame(gvkeyiid, ddt, M_RETURN)
        month_date: 수익률 측정 대상 월 (YYYY-MM-DD)
        weight_col: 비중 컬럼명
        cost_bps: 거래비용 (bp)
        prev_weights_df: 이전 월 비중 (턴오버 계산용)

    Returns:
        해당 월 포트폴리오 수익률 (float)
    """
    if weights_df.empty:
        return 0.0

    # 해당 월 수익률 병합
    month_ret = mreturn_df[mreturn_df["ddt"] == month_date][["gvkeyiid", "M_RETURN"]].copy()
    merged = weights_df[["gvkeyiid", weight_col]].merge(month_ret, on="gvkeyiid", how="left")
    merged["M_RETURN"] = merged["M_RETURN"].fillna(0.0)

    # 가중 수익률
    gross_return = (merged[weight_col] * merged["M_RETURN"]).sum()

    # 거래비용: 턴오버 기반
    if prev_weights_df is not None and not prev_weights_df.empty:
        all_ids = set(merged["gvkeyiid"]) | set(prev_weights_df["gvkeyiid"])
        curr = merged.set_index("gvkeyiid")[weight_col].reindex(all_ids, fill_value=0.0)
        prev = prev_weights_df.set_index("gvkeyiid")[weight_col].reindex(all_ids, fill_value=0.0)
        turnover = (curr - prev).abs().sum() / 2.0
    else:
        turnover = weights_df[weight_col].abs().sum() / 2.0  # 초기 진입

    trading_cost = turnover * (cost_bps / 10000.0)
    net_return = gross_return - trading_cost

    return net_return


# ─────────────────────────────────────────────────────────────────────────────
# Expanding Window 백테스트 엔진
# ─────────────────────────────────────────────────────────────────────────────
class ExpandingWindowBacktest:
    """기존 팩터 파이프라인 + TFT를 expanding window로 실행."""

    def __init__(
        self,
        config: dict,
        factor_info_path: str | Path,
        initial_train_end: str = "2022-12-31",
        backtest_start: str = "2023-01-31",
        backtest_end: str = "2025-12-31",
        mode: str = "satellite",
        core_weight: float = 0.80,
        satellite_weight: float = 0.20,
    ):
        """
        Args:
            config: 전체 설정 dict (PARAM 등)
            factor_info_path: factor_info.csv 경로
            initial_train_end: 최초 학습 종료 시점
            backtest_start: OOS 백테스트 시작 월
            backtest_end: OOS 백테스트 종료 월
            mode: "static" | "factor-timing" | "satellite" | "hybrid" | "compare"
            core_weight: Core 비중 (satellite/hybrid 모드)
            satellite_weight: Satellite 비중
        """
        self.config = config
        self.factor_info_path = Path(factor_info_path)
        self.initial_train_end = pd.Timestamp(initial_train_end)
        self.backtest_start = pd.Timestamp(backtest_start)
        self.backtest_end = pd.Timestamp(backtest_end)
        self.mode = mode
        self.core_weight = core_weight
        self.satellite_weight = satellite_weight

    def _generate_monthly_dates(self) -> List[pd.Timestamp]:
        """backtest_start ~ backtest_end 사이의 월말 날짜 리스트."""
        dates = pd.date_range(
            start=self.backtest_start,
            end=self.backtest_end,
            freq="ME",
        )
        return list(dates)

    def run(self) -> BacktestResult | CompareResult:
        """Expanding window 루프 실행.

        Returns:
            BacktestResult (단일 모드) 또는 CompareResult (compare 모드)
        """
        if self.mode == "compare":
            return self._run_compare()
        return self._run_single(self.mode)

    def _run_compare(self) -> CompareResult:
        """4가지 모드 모두 실행 → CompareResult."""
        results = {}
        for mode in ["static", "factor-timing", "satellite", "hybrid"]:
            logger.info("=" * 60)
            logger.info("[Compare] Mode: %s 실행 시작", mode)
            logger.info("=" * 60)
            try:
                results[mode] = self._run_single(mode)
            except Exception as e:
                logger.error("[Compare] Mode %s 실패: %s", mode, e)
                continue

        return CompareResult(results=results)

    def _run_single(self, mode: str) -> BacktestResult:
        """단일 모드 expanding window 백테스트.

        이 메서드는 기존 파이프라인과 TFT 모듈을 통합하는 오케스트레이터이다.
        실제 실행 시에는 기존 ModelPortfolioPipeline과 TFT 모듈이 필요하다.
        """
        from service.pipeline.model_portfolio import ModelPortfolioPipeline
        from service.pipeline.blending import blend_portfolios

        monthly_dates = self._generate_monthly_dates()
        logger.info(
            "[ExpandingWindow] mode=%s, %d months (%s ~ %s)",
            mode, len(monthly_dates),
            monthly_dates[0].strftime("%Y-%m-%d"),
            monthly_dates[-1].strftime("%Y-%m-%d"),
        )

        # TFT 모델 초기화 (필요 시)
        stock_tft = None
        factor_tft = None
        stock_preparator = None
        factor_preparator = None
        prev_checkpoint_stock = None
        prev_checkpoint_factor = None

        if mode in ("satellite", "hybrid"):
            from service.pipeline.tft_satellite import TFTModel, TFTDataPreparator
            stock_tft = TFTModel()
            # parquet 경로는 config에서 가져오거나 기본값 사용
            benchmark = self.config.get("benchmark", "MXCN1A")
            data_dir = Path("data")
            stock_preparator = TFTDataPreparator(
                factor_parquet_path=data_dir / f"{benchmark}_factor.parquet",
                mreturn_parquet_path=data_dir / f"{benchmark}_mreturn.parquet",
                factor_info_path=self.factor_info_path,
            )

        if mode in ("factor-timing", "hybrid"):
            from service.pipeline.tft_factor_timing import (
                FactorTimingTFT,
                FactorTimingDataPreparator,
                predicted_spreads_to_factor_weights,
                factor_weights_to_stock_weights,
            )
            factor_tft = FactorTimingTFT()
            factor_preparator = FactorTimingDataPreparator(self.factor_info_path)

        # 수익률 기록
        records = []
        weight_history: Dict[str, pd.DataFrame] = {}
        factor_weight_history: Dict[str, pd.DataFrame] = {}
        prev_blended_weights = None

        # M_RETURN 로드 (포트폴리오 수익률 계산용)
        benchmark = self.config.get("benchmark", "MXCN1A")
        mreturn_path = Path("data") / f"{benchmark}_mreturn.parquet"
        if mreturn_path.exists():
            mreturn_df = pd.read_parquet(mreturn_path)
            mreturn_df["ddt"] = pd.to_datetime(mreturn_df["ddt"])
        else:
            # test_data.csv에서 로드 등 대안
            logger.warning("[ExpandingWindow] mreturn parquet 없음, 수익률 계산 불가")
            mreturn_df = pd.DataFrame(columns=["gvkeyiid", "ddt", "M_RETURN"])

        for i, month_t in enumerate(monthly_dates):
            train_end = month_t - pd.DateOffset(months=1)
            train_end_str = train_end.strftime("%Y-%m-%d")
            month_str = month_t.strftime("%Y-%m-%d")
            window_num = i + 1

            logger.info(
                "[Window %d/%d] train_end=%s, predict=%s",
                window_num, len(monthly_dates), train_end_str, month_str,
            )

            # ─── Step 1~3: 모든 모드에서 공통 (기존 파이프라인) ───
            pipeline = ModelPortfolioPipeline(
                config=self.config,
                factor_info_path=self.factor_info_path,
                is_test=True,
            )

            try:
                # 기존 파이프라인 Step 1~3만 실행
                start_date = "2017-12-31"
                raw_data, market_return_df, start_date, _ = pipeline._load_data(
                    start_date, train_end_str, test_file=None,
                )
                factor_metadata, merged_data, factor_abbr_list, orders = pipeline._prepare_metadata(
                    raw_data, market_return_df,
                )
                factor_stats = pipeline._analyze_factors(
                    merged_data, factor_abbr_list, orders, test_file=None,
                )
                from service.pipeline.factor_analysis import filter_and_label_factors
                factor_names = factor_metadata.set_index("factorAbbreviation")["factorName"].to_dict()
                factor_styles = factor_metadata.set_index("factorAbbreviation")["styleName"].to_dict()
                name_list = [factor_names.get(a, a) for a in factor_abbr_list]
                style_list_full = [factor_styles.get(a, "Unknown") for a in factor_abbr_list]

                kept_abbrs, kept_names, kept_styles, _, _, filtered_data = filter_and_label_factors(
                    factor_abbr_list, name_list, style_list_full, factor_stats,
                )
            except Exception as e:
                logger.error("[Window %d] Step 1~3 실패: %s", window_num, e)
                records.append({"date": month_t, "core": 0.0, "satellite": 0.0, "blended": 0.0})
                continue

            # ─── Core MP 산출 (mode에 따라 분기) ───
            core_weights = pd.DataFrame(columns=["gvkeyiid", "ticker", "isin", "stock_weight"])

            if mode in ("static", "satellite"):
                try:
                    # 기존 Step 4~6
                    from service.pipeline.pipeline_utils import aggregate_factor_returns
                    from service.pipeline.correlation import calculate_downside_correlation
                    from service.pipeline.optimization import (
                        find_optimal_mix,
                        simulate_constrained_weights,
                    )

                    return_matrix, correlation_matrix, meta = pipeline._evaluate_universe(
                        kept_abbrs, kept_names, kept_styles, filtered_data, test_file=None,
                    )

                    if return_matrix is not None and not return_matrix.empty:
                        best_sub, ret_subset, factor_list, style_list = pipeline._optimize_mixes(
                            return_matrix, meta, correlation_matrix,
                        )
                        sim_result = simulate_constrained_weights(
                            ret_subset, style_list, mode="simulation", test_mode=True,
                        )

                        # Step 7: 종목 비중 구성 (CSV 출력 대신 메모리에서)
                        factor_idx_map = {fac: idx for idx, fac in enumerate(kept_abbrs)}
                        sim_factors = sim_result[1][["factor", "fitted_weight", "styleName"]].to_dict("records")

                        weight_frames = []
                        for row in sim_factors:
                            fac, w, s = row["factor"], row["fitted_weight"], row["styleName"]
                            if fac not in factor_idx_map:
                                continue
                            j = factor_idx_map[fac]
                            df = filtered_data[j]
                            df = df.loc[df["ddt"] == train_end_str,
                                        ["ddt", "ticker", "isin", "gvkeyiid", "label"]].copy()
                            if df.empty:
                                continue
                            count_per_group = df.groupby("label")["label"].transform("count")
                            df["stock_weight"] = df["label"] * w / count_per_group
                            weight_frames.append(df[["gvkeyiid", "ticker", "isin", "stock_weight"]])

                        if weight_frames:
                            core_weights = pd.concat(weight_frames, ignore_index=True)
                            core_weights = core_weights.groupby(
                                ["gvkeyiid", "ticker", "isin"], as_index=False
                            )["stock_weight"].sum()

                except Exception as e:
                    logger.error("[Window %d] Core (static) 실패: %s", window_num, e)

            elif mode in ("factor-timing", "hybrid"):
                try:
                    from service.pipeline.pipeline_utils import aggregate_factor_returns
                    from service.pipeline.tft_factor_timing import (
                        predicted_spreads_to_factor_weights,
                        factor_weights_to_stock_weights,
                    )

                    # factor_spread_returns 전체 시계열 생성
                    factor_spread_returns = aggregate_factor_returns(filtered_data, kept_abbrs)

                    # Factor Timing TFT 학습/예측
                    factor_panel = factor_preparator.prepare(factor_spread_returns)

                    if len(factor_panel) > 0:
                        training_cutoff = factor_panel["time_idx"].max()

                        # Cold/Warm start 결정
                        is_cold = (
                            prev_checkpoint_factor is None
                            or (window_num - 1) % factor_tft.config["cold_start_interval"] == 0
                        )

                        train_ds, val_ds = factor_tft.build_dataset(
                            factor_panel, training_cutoff,
                            macro_cols=factor_preparator.macro_cols,
                        )

                        ckpt = None if is_cold else prev_checkpoint_factor
                        model = factor_tft.train(train_ds, val_ds, prev_checkpoint=ckpt)

                        # 예측
                        pred_ds = factor_tft.from_training_dataset(train_ds, factor_panel)
                        preds = factor_tft.predict(model, pred_ds, factor_panel)

                        # 체크포인트 저장
                        prev_checkpoint_factor = factor_tft.save_checkpoint(model, train_end_str)

                        # 팩터 비중 산출
                        style_info = pd.read_csv(self.factor_info_path)[["factorAbbreviation", "styleName"]]
                        factor_weights = predicted_spreads_to_factor_weights(preds, style_info)

                        factor_weight_history[month_str] = factor_weights.copy()

                        # 종목 비중 변환
                        core_weights = factor_weights_to_stock_weights(
                            factor_weights, filtered_data, kept_abbrs, train_end_str,
                        )

                except Exception as e:
                    logger.error("[Window %d] Core (factor-timing) 실패: %s", window_num, e)

            # ─── Satellite MP 산출 (mode에 따라 분기) ───
            sat_weights = pd.DataFrame(columns=["gvkeyiid", "tft_weight"])

            if mode in ("satellite", "hybrid") and stock_tft is not None:
                try:
                    from service.pipeline.tft_satellite import prediction_to_weights

                    stock_panel = stock_preparator.prepare()

                    if len(stock_panel) > 0:
                        training_cutoff = stock_panel["time_idx"].max()

                        is_cold = (
                            prev_checkpoint_stock is None
                            or (window_num - 1) % stock_tft.config["cold_start_interval"] == 0
                        )

                        train_ds, val_ds = stock_tft.build_dataset(stock_panel, training_cutoff)

                        ckpt = None if is_cold else prev_checkpoint_stock
                        model = stock_tft.train(train_ds, val_ds, prev_checkpoint=ckpt)

                        # 예측
                        pred_ds = stock_tft.from_training_dataset(train_ds, stock_panel)
                        preds = stock_tft.predict(model, pred_ds, stock_panel)

                        prev_checkpoint_stock = stock_tft.save_checkpoint(model, train_end_str)

                        sat_weights = prediction_to_weights(preds)

                except Exception as e:
                    logger.error("[Window %d] Satellite 실패: %s", window_num, e)

            # ─── Blending ───
            has_satellite = not sat_weights.empty and "tft_weight" in sat_weights.columns
            if has_satellite:
                blended_weights = blend_portfolios(
                    core_weights.rename(columns={"stock_weight": "core_weight"}),
                    sat_weights,
                    core_ratio=self.core_weight,
                    tft_ratio=self.satellite_weight,
                )
            else:
                blended_weights = core_weights.copy()
                if "stock_weight" in blended_weights.columns:
                    blended_weights = blended_weights.rename(columns={"stock_weight": "blended_weight"})

            # ─── OOS 수익률 기록 ───
            weight_col = "blended_weight" if "blended_weight" in blended_weights.columns else "stock_weight"

            blended_return = _calculate_portfolio_return(
                blended_weights, mreturn_df, month_str,
                weight_col=weight_col,
                prev_weights_df=prev_blended_weights,
            )

            core_return = _calculate_portfolio_return(
                core_weights, mreturn_df, month_str,
                weight_col="stock_weight",
            )

            sat_return = 0.0
            if has_satellite:
                sat_return = _calculate_portfolio_return(
                    sat_weights.rename(columns={"tft_weight": "stock_weight"}),
                    mreturn_df, month_str,
                    weight_col="stock_weight",
                )

            records.append({
                "date": month_t,
                "core": core_return,
                "satellite": sat_return,
                "blended": blended_return,
            })

            prev_blended_weights = blended_weights.rename(
                columns={weight_col: "stock_weight"}
            ) if weight_col != "stock_weight" else blended_weights
            weight_history[month_str] = blended_weights.copy()

            logger.info(
                "[Window %d] Return: core=%.4f, sat=%.4f, blended=%.4f",
                window_num, core_return, sat_return, blended_return,
            )

        # ─── 결과 집계 ───
        monthly_df = pd.DataFrame(records)
        if monthly_df.empty:
            monthly_df = pd.DataFrame(columns=["date", "core", "satellite", "blended"])

        # 누적 수익률
        cum_df = pd.DataFrame(index=monthly_df["date"])
        for col in ["core", "satellite", "blended"]:
            if col in monthly_df.columns:
                cum_df[col] = (1 + monthly_df[col].values).cumprod()
        cum_df.index.name = "date"

        # 성과 지표 (blended 기준)
        blended_series = monthly_df["blended"] if "blended" in monthly_df.columns else pd.Series(dtype=float)
        stats = calculate_performance_metrics(blended_series)

        result = BacktestResult(
            mode=mode,
            monthly_returns=monthly_df,
            cumulative_returns=cum_df,
            stats=stats,
            monthly_weights=weight_history,
            factor_weights_history=factor_weight_history,
        )

        # 자동 저장
        result.save()
        logger.info("[ExpandingWindow] mode=%s 완료. Stats: %s", mode, stats)

        return result
