"""Feature engineering shared by NEM forecast training and the 5-minute inference job.

Every feature at time t uses only data published at or before t, so the same code path is valid for
back-testing and for live inference. All inputs come from gold.nem_prices_5min (real AEMO NEMWEB data).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

REGIONS: tuple[str, ...] = ("NSW1", "QLD1", "VIC1", "SA1", "TAS1")
PRICE_LAGS: tuple[int, ...] = (0, 1, 2, 3, 6, 12, 24, 48, 288)  # 5-minute intervals; 288 = same time yesterday
DEMAND_LAGS: tuple[int, ...] = (0, 1, 12, 288)
INTERVAL = pd.Timedelta(minutes=5)
MARKET_TZ_OFFSET = pd.Timedelta(hours=10)  # NEM market time (AEST, no daylight saving)


def load_wide(spark, catalog: str, start_utc: str) -> pd.DataFrame:
    """Gold prices pivoted to one row per 5-minute interval (UTC) with per-region columns, on a full grid."""
    df = spark.sql(f"""
        SELECT interval_datetime, region_id, rrp, total_demand_mw, available_gen_mw, net_interchange_mw
        FROM {catalog}.gold.nem_prices_5min
        WHERE interval_datetime >= TIMESTAMP '{start_utc}'
    """).toPandas()
    if df.empty:
        raise ValueError(f"No rows in {catalog}.gold.nem_prices_5min since {start_utc}")
    df["interval_datetime"] = pd.to_datetime(df["interval_datetime"])
    wide = df.pivot_table(index="interval_datetime", columns="region_id",
                          values=["rrp", "total_demand_mw", "available_gen_mw", "net_interchange_mw"])
    wide.columns = [f"{value}_{region}" for value, region in wide.columns]
    grid = pd.date_range(wide.index.min(), wide.index.max(), freq=INTERVAL)
    return wide.reindex(grid)  # missing intervals stay NaN so that shift() lags remain time-correct


def region_features(wide: pd.DataFrame, region: str) -> pd.DataFrame:
    """Features known at each interval t for one region."""
    price = wide[f"rrp_{region}"]
    demand = wide[f"total_demand_mw_{region}"]
    cols: dict[str, pd.Series] = {}
    for lag in PRICE_LAGS:
        cols[f"rrp_lag_{lag}"] = price.shift(lag)
    for window in (12, 288):
        cols[f"rrp_mean_{window}"] = price.rolling(window, min_periods=window // 2).mean()
        cols[f"rrp_std_{window}"] = price.rolling(window, min_periods=window // 2).std()
        cols[f"rrp_max_{window}"] = price.rolling(window, min_periods=window // 2).max()
    for lag in DEMAND_LAGS:
        cols[f"demand_lag_{lag}"] = demand.shift(lag)
    cols["demand_change_12"] = demand - demand.shift(12)
    cols["supply_margin_mw"] = wide[f"available_gen_mw_{region}"] - demand
    cols["net_interchange_mw"] = wide[f"net_interchange_mw_{region}"]
    others = [r for r in REGIONS if r != region and f"rrp_{r}" in wide]
    for other in others:
        cols[f"rrp_{other}"] = wide[f"rrp_{other}"]
    cols["spread_to_neighbours"] = price - wide[[f"rrp_{o}" for o in others]].mean(axis=1)

    market_time = wide.index + MARKET_TZ_OFFSET
    minute_of_day = market_time.hour * 60 + market_time.minute
    cols["mod_sin"] = pd.Series(np.sin(2 * np.pi * minute_of_day / 1440), index=wide.index)
    cols["mod_cos"] = pd.Series(np.cos(2 * np.pi * minute_of_day / 1440), index=wide.index)
    cols["hour"] = pd.Series(market_time.hour, index=wide.index)
    cols["day_of_week"] = pd.Series(market_time.dayofweek, index=wide.index)
    return pd.DataFrame(cols, index=wide.index)


def stack_horizons(feats: pd.DataFrame, wide: pd.DataFrame, region: str, horizons: tuple[int, ...]) -> pd.DataFrame:
    """One row per (issue time t, horizon h) with targets at t + h and the persistence baseline."""
    frames = []
    for h in horizons:
        frame = feats.copy()
        frame["horizon"] = h
        frame["issue_time"] = feats.index
        frame["target_time"] = feats.index + h * INTERVAL
        frame["y_price"] = wide[f"rrp_{region}"].shift(-h).values
        frame["y_demand"] = wide[f"total_demand_mw_{region}"].shift(-h).values
        frame["persistence_price"] = wide[f"rrp_{region}"].values
        frame["persistence_demand"] = wide[f"total_demand_mw_{region}"].values
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


NON_FEATURES = ("issue_time", "target_time", "y_price", "y_demand", "persistence_price", "persistence_demand")


def feature_columns(stacked: pd.DataFrame) -> list[str]:
    return [c for c in stacked.columns if c not in NON_FEATURES]
