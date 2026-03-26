# -*- coding: utf-8 -*-
"""TFT Factor Timing (접근 A — Core 대체 옵션) 모듈.

기존 Step 1~3의 결과물인 팩터별 롱-숏 스프레드 수익률 시계열을 입력으로 받아,
다음 달 어떤 팩터가 잘 될지 예측하고 팩터 비중을 동적으로 결정한다.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
import lightning.pytorch as pl
from lightning.pytorch.callbacks import EarlyStopping, ModelCheckpoint, LearningRateMonitor
from pytorch_forecasting import TemporalFusionTransformer, TimeSeriesDataSet
from pytorch_forecasting.data.encoders import NaNLabelEncoder
from pytorch_forecasting.metrics import MAE

from service.pipeline.tft_satellite import _set_seed, filter_short_series

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# 기본 설정
# ─────────────────────────────────────────────────────────────────────────────
TFT_FACTOR_TIMING_CONFIG = {
    "hidden_size": 16,
    "attention_head_size": 1,
    "lstm_layers": 1,
    "dropout": 0.4,
    "learning_rate": 1e-3,
    "weight_decay": 1e-4,
    "max_epochs": 80,
    "batch_size": 32,
    "max_encoder_length": 12,
    "max_prediction_length": 1,
    "early_stop_patience": 10,
    "gradient_clip_val": 0.1,
    "warm_start": True,
    "warm_start_epochs": 15,
    "warm_start_lr_factor": 0.2,
    "cold_start_interval": 6,
    "min_series_length": 13,
    "max_style_weight": 0.25,
    "min_factor_weight": 0.0,
    "factor_weight_method": "rank",
    "softmax_temperature": 1.0,
    "seed": 42,
}

CHECKPOINT_DIR = Path("checkpoints/tft_factor")


# ─────────────────────────────────────────────────────────────────────────────
# 데이터 전처리
# ─────────────────────────────────────────────────────────────────────────────
class FactorTimingDataPreparator:
    """기존 파이프라인 Step 1~3 결과를 Factor Timing TFT 입력으로 변환."""

    def __init__(self, factor_info_path: str | Path):
        self.factor_info_path = Path(factor_info_path)

    def prepare(
        self,
        factor_spread_returns: pd.DataFrame,
        macro_features: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """팩터별 스프레드 수익률 시계열 → TFT 학습용 panel.

        Args:
            factor_spread_returns: DataFrame(ddt × factorAbbreviation)
                — 기존 pipeline_utils.aggregate_factor_returns() 출력
                — 행=월(DatetimeIndex), 열=팩터별 net 스프레드 수익률
            macro_features: (선택) DataFrame(ddt, feature_1, feature_2, ...)

        Returns:
            wide-format panel DataFrame:
                columns: factorAbbreviation, ddt, time_idx, net_spread_return,
                         rolling_mean_3m, rolling_mean_6m, rolling_vol_6m,
                         momentum_12m, drawdown, styleName, month_sin, month_cos,
                         quarter, [macro cols], target_spread_return_next
        """
        logger.info("[FactorTimingDataPreparator] 데이터 준비 시작")

        # factor_info에서 styleName 매핑
        factor_info = pd.read_csv(self.factor_info_path)
        style_map = factor_info.set_index("factorAbbreviation")["styleName"].to_dict()

        # Long format으로 변환: (factorAbbreviation, ddt) → net_spread_return
        ret = factor_spread_returns.copy()
        if not isinstance(ret.index, pd.DatetimeIndex):
            ret.index = pd.to_datetime(ret.index)
        ret.index.name = "ddt"

        records = []
        for factor_abbr in ret.columns:
            series = ret[factor_abbr].dropna()
            if len(series) < 6:
                continue

            fdf = pd.DataFrame({
                "ddt": series.index,
                "factorAbbreviation": factor_abbr,
                "net_spread_return": series.values,
            })
            records.append(fdf)

        panel = pd.concat(records, ignore_index=True)
        panel["ddt"] = pd.to_datetime(panel["ddt"])
        panel = panel.sort_values(["factorAbbreviation", "ddt"]).reset_index(drop=True)

        # 시계열 파생 피처 생성 (팩터별)
        logger.info("[FactorTimingDataPreparator] 파생 피처 생성")
        panel["rolling_mean_3m"] = panel.groupby("factorAbbreviation")["net_spread_return"].transform(
            lambda x: x.rolling(3, min_periods=2).mean()
        )
        panel["rolling_mean_6m"] = panel.groupby("factorAbbreviation")["net_spread_return"].transform(
            lambda x: x.rolling(6, min_periods=3).mean()
        )
        panel["rolling_vol_6m"] = panel.groupby("factorAbbreviation")["net_spread_return"].transform(
            lambda x: x.rolling(6, min_periods=3).std()
        )
        panel["momentum_12m"] = panel.groupby("factorAbbreviation")["net_spread_return"].transform(
            lambda x: (1 + x).rolling(12, min_periods=6).apply(lambda w: w.prod() - 1, raw=True)
        )

        # Drawdown: 누적 수익률의 고점 대비 하락폭
        def _drawdown(x: pd.Series) -> pd.Series:
            cum = (1 + x).cumprod()
            running_max = cum.cummax()
            dd = (cum / running_max) - 1
            return dd

        panel["drawdown"] = panel.groupby("factorAbbreviation")["net_spread_return"].transform(_drawdown)

        # 타겟 생성: 다음 달 스프레드 수익률
        panel["target_spread_return_next"] = panel.groupby("factorAbbreviation")["net_spread_return"].shift(-1)

        # 스타일 분류 (static categorical)
        panel["styleName"] = panel["factorAbbreviation"].map(style_map).fillna("Unknown")

        # 캘린더 피처
        panel["month"] = panel["ddt"].dt.month
        panel["month_sin"] = np.sin(2 * np.pi * panel["month"] / 12)
        panel["month_cos"] = np.cos(2 * np.pi * panel["month"] / 12)
        panel["quarter"] = panel["ddt"].dt.quarter

        # 매크로 변수 병합 (선택사항)
        self._macro_cols: List[str] = []
        if macro_features is not None:
            macro_features = macro_features.copy()
            macro_features["ddt"] = pd.to_datetime(macro_features["ddt"])
            self._macro_cols = [c for c in macro_features.columns if c != "ddt"]
            panel = panel.merge(macro_features, on="ddt", how="left")
            for mc in self._macro_cols:
                panel[mc] = panel[mc].fillna(method="ffill").fillna(0.0)

        # NaN 처리: rolling/momentum/drawdown 초기 구간 NaN → 행 제거
        derived_cols = [
            "rolling_mean_3m", "rolling_mean_6m", "rolling_vol_6m",
            "momentum_12m", "drawdown",
        ]
        for col in derived_cols:
            panel[col] = panel[col].fillna(0.0)

        # 타겟 결측 제거 (마지막 월)
        panel = panel.dropna(subset=["target_spread_return_next"]).copy()

        # time_idx 생성
        all_dates = sorted(panel["ddt"].unique())
        date_to_idx = {d: i for i, d in enumerate(all_dates)}
        panel["time_idx"] = panel["ddt"].map(date_to_idx)

        # 문자열 변환
        panel["factorAbbreviation"] = panel["factorAbbreviation"].astype(str)
        panel["styleName"] = panel["styleName"].astype(str)

        # 짧은 시계열 필터 (팩터 기준)
        min_length = TFT_FACTOR_TIMING_CONFIG["min_series_length"]
        panel = filter_short_series(
            panel, min_length, time_col="time_idx", group_col="factorAbbreviation"
        )

        # month 컬럼 제거 (sin/cos로 대체)
        panel = panel.drop(columns=["month"], errors="ignore")

        logger.info(
            "[FactorTimingDataPreparator] 최종 패널: %d rows, %d 팩터, %d 월",
            len(panel),
            panel["factorAbbreviation"].nunique(),
            panel["time_idx"].nunique(),
        )
        return panel

    @property
    def macro_cols(self) -> List[str]:
        """마지막 prepare() 호출에서 사용된 매크로 컬럼 리스트."""
        return getattr(self, "_macro_cols", [])


# ─────────────────────────────────────────────────────────────────────────────
# Factor Timing TFT 모델
# ─────────────────────────────────────────────────────────────────────────────
class FactorTimingTFT:
    """팩터 스프레드 수익률 예측용 TFT — 접근 C보다 작은 모델."""

    def __init__(self, config: Optional[Dict] = None):
        self.config = {**TFT_FACTOR_TIMING_CONFIG, **(config or {})}
        self._last_model_path: Optional[str] = None
        self._training_dataset: Optional[TimeSeriesDataSet] = None

    def build_dataset(
        self,
        df: pd.DataFrame,
        training_cutoff: int,
        macro_cols: Optional[List[str]] = None,
    ) -> Tuple[TimeSeriesDataSet, TimeSeriesDataSet]:
        """TimeSeriesDataSet 생성 — 팩터가 group_id.

        Args:
            df: FactorTimingDataPreparator.prepare() 출력
            training_cutoff: time_idx 기준 전체 데이터의 마지막 인덱스
            macro_cols: 매크로 변수 컬럼 리스트

        Returns:
            (training_dataset, validation_dataset)
        """
        cfg = self.config

        time_varying_unknown = [
            "net_spread_return", "rolling_mean_3m", "rolling_mean_6m",
            "rolling_vol_6m", "momentum_12m", "drawdown",
        ]
        time_varying_known = ["month_sin", "month_cos", "quarter"]
        if macro_cols:
            time_varying_known.extend(macro_cols)

        # 시간 기준 분할: 마지막 12개월 = validation
        val_months = 12
        train_cutoff = training_cutoff - val_months

        train_df = df[df["time_idx"] <= training_cutoff].copy()

        training = TimeSeriesDataSet(
            train_df[train_df["time_idx"] <= train_cutoff],
            time_idx="time_idx",
            target="target_spread_return_next",
            group_ids=["factorAbbreviation"],
            static_categoricals=["styleName"],
            time_varying_known_reals=time_varying_known,
            time_varying_unknown_reals=time_varying_unknown,
            max_encoder_length=cfg["max_encoder_length"],
            max_prediction_length=cfg["max_prediction_length"],
            min_encoder_length=cfg["max_encoder_length"] // 2,
            categorical_encoders={
                "styleName": NaNLabelEncoder(add_nan=True),
                "factorAbbreviation": NaNLabelEncoder(add_nan=True),
            },
            allow_missing_timesteps=True,
        )

        validation = TimeSeriesDataSet.from_dataset(
            training,
            train_df[train_df["time_idx"] > train_cutoff],
            stop_randomization=True,
        )

        self._training_dataset = training
        return training, validation

    def from_training_dataset(
        self,
        training_dataset: TimeSeriesDataSet,
        predict_df: pd.DataFrame,
    ) -> TimeSeriesDataSet:
        """학습 데이터셋의 인코더를 상속받아 예측용 데이터셋 생성."""
        return TimeSeriesDataSet.from_dataset(
            training_dataset, predict_df, stop_randomization=True,
        )

    def train(
        self,
        train_dataset: TimeSeriesDataSet,
        val_dataset: TimeSeriesDataSet,
        prev_checkpoint: Optional[str] = None,
    ) -> TemporalFusionTransformer:
        """OOM 방어 포함 학습 실행."""
        return self._train_with_oom_retry(train_dataset, val_dataset, prev_checkpoint)

    def _train_with_oom_retry(
        self,
        train_dataset: TimeSeriesDataSet,
        val_dataset: TimeSeriesDataSet,
        prev_checkpoint: Optional[str] = None,
    ) -> TemporalFusionTransformer:
        """OOM 발생 시 batch_size를 절반으로 줄여가며 재시도."""
        batch_size = self.config["batch_size"]
        min_batch_size = 8  # 접근 A는 데이터가 적으므로 최소값 더 낮음

        while batch_size >= min_batch_size:
            try:
                return self._train_impl(
                    train_dataset, val_dataset,
                    batch_size=batch_size,
                    prev_checkpoint=prev_checkpoint,
                )
            except RuntimeError as e:
                if "out of memory" in str(e).lower() or "mps" in str(e).lower():
                    batch_size //= 2
                    logger.warning(
                        "[FactorTimingTFT] OOM 발생 → batch_size=%d로 재시도", batch_size
                    )
                    if torch.cuda.is_available():
                        torch.cuda.empty_cache()
                else:
                    raise
        raise RuntimeError(
            f"batch_size={min_batch_size}에서도 OOM 발생. 모델 축소 필요."
        )

    def _train_impl(
        self,
        train_dataset: TimeSeriesDataSet,
        val_dataset: TimeSeriesDataSet,
        batch_size: int,
        prev_checkpoint: Optional[str] = None,
    ) -> TemporalFusionTransformer:
        """실제 학습 로직."""
        _set_seed(self.config["seed"])
        cfg = self.config

        train_dl = train_dataset.to_dataloader(
            train=True, batch_size=batch_size, num_workers=0,
        )
        val_dl = val_dataset.to_dataloader(
            train=False, batch_size=batch_size, num_workers=0,
        )

        is_warm = prev_checkpoint is not None and cfg["warm_start"]
        max_epochs = cfg["warm_start_epochs"] if is_warm else cfg["max_epochs"]
        lr = cfg["learning_rate"] * cfg["warm_start_lr_factor"] if is_warm else cfg["learning_rate"]

        if is_warm and Path(prev_checkpoint).exists():
            logger.info("[FactorTimingTFT] Warm-start: %s 로드", prev_checkpoint)
            tft = TemporalFusionTransformer.load_from_checkpoint(prev_checkpoint)
            tft.hparams.learning_rate = lr
        else:
            if is_warm:
                logger.warning(
                    "[FactorTimingTFT] 체크포인트 %s 없음 → Cold start", prev_checkpoint
                )
            tft = TemporalFusionTransformer.from_dataset(
                train_dataset,
                hidden_size=cfg["hidden_size"],
                attention_head_size=cfg["attention_head_size"],
                lstm_layers=cfg["lstm_layers"],
                dropout=cfg["dropout"],
                learning_rate=lr,
                output_size=1,
                loss=MAE(),
                log_interval=10,
                reduce_on_plateau_patience=5,
                weight_decay=cfg["weight_decay"],
            )

        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        checkpoint_cb = ModelCheckpoint(
            dirpath=str(CHECKPOINT_DIR),
            filename="best-{epoch}-{val_loss:.4f}",
            monitor="val_loss",
            mode="min",
            save_top_k=1,
        )
        early_stop_cb = EarlyStopping(
            monitor="val_loss",
            patience=cfg["early_stop_patience"],
            mode="min",
        )
        lr_monitor = LearningRateMonitor(logging_interval="epoch")

        trainer = pl.Trainer(
            max_epochs=max_epochs,
            accelerator="auto",
            gradient_clip_val=cfg["gradient_clip_val"],
            callbacks=[checkpoint_cb, early_stop_cb, lr_monitor],
            enable_progress_bar=True,
            log_every_n_steps=5,
        )

        trainer.fit(tft, train_dataloaders=train_dl, val_dataloaders=val_dl)

        best_path = checkpoint_cb.best_model_path
        if best_path:
            self._last_model_path = best_path
            logger.info("[FactorTimingTFT] Best checkpoint: %s", best_path)
            return TemporalFusionTransformer.load_from_checkpoint(best_path)

        self._last_model_path = None
        return tft

    @property
    def last_checkpoint_path(self) -> Optional[str]:
        return self._last_model_path

    def predict(
        self,
        model: TemporalFusionTransformer,
        predict_dataset: TimeSeriesDataSet,
        df_with_ids: pd.DataFrame,
    ) -> pd.DataFrame:
        """팩터별 예측 스프레드 수익률 반환.

        Returns:
            DataFrame(factorAbbreviation, predicted_spread)
        """
        predict_dl = predict_dataset.to_dataloader(
            train=False, batch_size=self.config["batch_size"], num_workers=0,
        )

        raw_preds = model.predict(predict_dl, mode="raw", return_x=True)
        predictions = raw_preds.output["prediction"].squeeze(-1).squeeze(-1).cpu().numpy()

        decoded = predict_dataset.decoded_index
        result_df = pd.DataFrame({
            "factorAbbreviation": decoded["factorAbbreviation"].values[:len(predictions)],
            "predicted_spread": predictions,
        })

        # 중복 팩터가 있을 경우 평균
        result_df = result_df.groupby("factorAbbreviation", as_index=False)["predicted_spread"].mean()

        return result_df

    def save_checkpoint(self, model: TemporalFusionTransformer, window_end: str) -> str:
        """특정 윈도우 엔드에 대한 체크포인트를 저장."""
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        path = str(CHECKPOINT_DIR / f"window_{window_end}.ckpt")
        trainer = pl.Trainer(accelerator="auto", logger=False)
        trainer.strategy.connect(model)
        trainer.save_checkpoint(path)
        self._last_model_path = path
        logger.info("[FactorTimingTFT] 체크포인트 저장: %s", path)
        return path


# ─────────────────────────────────────────────────────────────────────────────
# 팩터 예측 → 팩터 비중 변환
# ─────────────────────────────────────────────────────────────────────────────
def predicted_spreads_to_factor_weights(
    predictions_df: pd.DataFrame,
    style_info_df: pd.DataFrame,
    max_style_weight: float = 0.25,
    min_factor_weight: float = 0.0,
    method: str = "rank",
    softmax_temperature: float = 1.0,
) -> pd.DataFrame:
    """TFT 예측 스프레드 수익률 → 팩터별 투자 비중.

    Args:
        predictions_df: DataFrame(factorAbbreviation, predicted_spread)
        style_info_df: DataFrame(factorAbbreviation, styleName)
        max_style_weight: 스타일당 최대 비중 (25%)
        min_factor_weight: 음수 예측 팩터 제외 여부
        method: "rank" (권장), "softmax", "proportional"
        softmax_temperature: softmax 방식의 온도

    Returns:
        DataFrame(factorAbbreviation, factor_weight) — 합계 = 1.0
    """
    df = predictions_df.merge(style_info_df, on="factorAbbreviation", how="left")

    # 양수 예측 팩터만 선정
    df = df[df["predicted_spread"] > min_factor_weight].copy()

    if df.empty:
        logger.warning("[Factor Timing] 양수 예측 팩터가 없음 → 빈 비중 반환")
        return pd.DataFrame(columns=["factorAbbreviation", "factor_weight"])

    # 비중 배분 방식
    if method == "rank":
        # 예측 수익률 순위 기반: 1등=N점, 2등=N-1점, ...
        n = len(df)
        df = df.sort_values("predicted_spread", ascending=False).reset_index(drop=True)
        df["rank_score"] = np.arange(n, 0, -1, dtype=float)
        df["raw_weight"] = df["rank_score"] / df["rank_score"].sum()

    elif method == "softmax":
        logits = df["predicted_spread"].values / softmax_temperature
        logits = logits - logits.max()  # numerical stability
        exp_logits = np.exp(logits)
        df["raw_weight"] = exp_logits / exp_logits.sum()

    elif method == "proportional":
        total = df["predicted_spread"].sum()
        if total > 0:
            df["raw_weight"] = df["predicted_spread"] / total
        else:
            df["raw_weight"] = 1.0 / len(df)
    else:
        raise ValueError(f"Unknown method: {method}")

    # 스타일 캡 적용 (max_style_weight)
    # 전략: 초과 스타일 → 캡으로 고정, 잔여를 미초과 스타일에 비례 재분배
    # 스타일 수 < 1/max_style_weight 이면 캡 적용 불가 (수학적 불가능)
    df["factor_weight"] = df["raw_weight"].copy()

    n_styles = df["styleName"].nunique()
    min_styles_for_cap = int(np.ceil(1.0 / max_style_weight))
    if n_styles < min_styles_for_cap:
        logger.info(
            "[Factor Timing] 스타일 %d개 < %d개 (1/%.0f%%) → 캡 적용 생략, 비례 배분만",
            n_styles, min_styles_for_cap, max_style_weight * 100,
        )
        total_w = df["factor_weight"].sum()
        if total_w > 0:
            df["factor_weight"] /= total_w
        return df[["factorAbbreviation", "factor_weight"]].copy()

    capped_styles: set = set()
    for _ in range(20):
        # 스타일별 합산
        style_totals = df.groupby("styleName")["factor_weight"].sum()
        over = style_totals[style_totals > max_style_weight + 1e-9]

        if over.empty:
            break

        # 새로 초과한 스타일들을 캡으로 클리핑
        for style_name in over.index:
            style_mask = df["styleName"] == style_name
            current_sum = df.loc[style_mask, "factor_weight"].sum()
            if current_sum > max_style_weight:
                df.loc[style_mask, "factor_weight"] *= max_style_weight / current_sum
            capped_styles.add(style_name)

        # 캡된 스타일 총합
        capped_total = sum(
            df.loc[df["styleName"] == s, "factor_weight"].sum() for s in capped_styles
        )
        remaining_budget = 1.0 - capped_total

        # 미캡 스타일에 잔여 예산 비례 분배
        uncapped_mask = ~df["styleName"].isin(capped_styles)
        uncapped_sum = df.loc[uncapped_mask, "factor_weight"].sum()

        if uncapped_sum > 0 and remaining_budget > 0:
            df.loc[uncapped_mask, "factor_weight"] *= remaining_budget / uncapped_sum
        else:
            # 모든 스타일이 캡됨 → 정규화 후 종료
            total_w = df["factor_weight"].sum()
            if total_w > 0:
                df["factor_weight"] /= total_w
            break

    # 최종 정규화 (부동소수점 보정)
    total_w = df["factor_weight"].sum()
    if total_w > 0:
        df["factor_weight"] /= total_w

    return df[["factorAbbreviation", "factor_weight"]].copy()


# ─────────────────────────────────────────────────────────────────────────────
# Factor Timing → 종목 비중 변환
# ─────────────────────────────────────────────────────────────────────────────
def factor_weights_to_stock_weights(
    factor_weights_df: pd.DataFrame,
    labeled_data_list: List[pd.DataFrame],
    factor_abbr_list: List[str],
    end_date: str,
) -> pd.DataFrame:
    """팩터 비중 → 종목별 최종 비중 (기존 Step 7 로직 재사용).

    Args:
        factor_weights_df: DataFrame(factorAbbreviation, factor_weight)
        labeled_data_list: 각 팩터의 라벨링된 종목 데이터 (Step 3 출력)
        factor_abbr_list: 팩터 약어 리스트
        end_date: 대상 시점 (YYYY-MM-DD)

    Returns:
        DataFrame(gvkeyiid, ticker, isin, stock_weight) — 기존 Step 7 CSV와 유사
    """
    factor_idx_map = {fac: idx for idx, fac in enumerate(factor_abbr_list)}
    weight_map = factor_weights_df.set_index("factorAbbreviation")["factor_weight"].to_dict()

    weight_frames = []
    for fac, w in weight_map.items():
        if fac not in factor_idx_map:
            continue
        if w <= 0:
            continue

        j = factor_idx_map[fac]
        if j >= len(labeled_data_list):
            continue

        df = labeled_data_list[j]
        df = df.loc[df["ddt"] == end_date, ["ddt", "ticker", "isin", "gvkeyiid", "label"]].copy()
        if df.empty:
            continue

        count_per_group = df.groupby("label")["label"].transform("count")

        # 롱/숏 종목군 내 동일 가중 → 팩터 비중만큼 스케일링
        df["stock_weight"] = df["label"] * w / count_per_group
        df["factor"] = fac
        df["factor_weight"] = w

        weight_frames.append(
            df[["ddt", "ticker", "isin", "gvkeyiid", "stock_weight", "factor", "factor_weight"]]
            .reset_index(drop=True)
        )

    if not weight_frames:
        logger.warning("[factor_weights_to_stock_weights] 유효한 팩터 없음")
        return pd.DataFrame(columns=["gvkeyiid", "ticker", "isin", "stock_weight"])

    result = pd.concat(weight_frames, ignore_index=True)

    # 종목별 비중 합산 → MP 구성
    agg = result.groupby(["ddt", "ticker", "isin", "gvkeyiid"], as_index=False)["stock_weight"].sum()

    return agg[["gvkeyiid", "ticker", "isin", "stock_weight"]].copy()
