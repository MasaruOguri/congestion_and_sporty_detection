from typing import Dict, List, Union
import pandas as pd
import numpy as np
from scipy.spatial import cKDTree


# POI半径内に車両緯度経度が含まれるか判定する関数
def judge_vehicle_in_radius(
    connected_data_pd_df,
    poi_data_pd_df,
    column_name={
        'latitude': 'LATITUDE',
        'longitude': 'LONGITUDE',
        'poi_name': 'NAME',
        'poi_latitude': 'LATITUDE',
        'poi_longitude': 'LONGITUDE',
        'poi_radius': 'RADIUS_M'
    },
    earth_radius=6371000.0,
    uniform_radius_m=None,
):
    result_pd_df = connected_data_pd_df.copy()
    connected_coords = np.radians(
        connected_data_pd_df[[column_name['latitude'],
                              column_name['longitude']]].to_numpy()
    )
    poi_coords = np.radians(
        poi_data_pd_df[[column_name['poi_latitude'],
                        column_name['poi_longitude']]].to_numpy()
    )
    tree = cKDTree(poi_coords)
    dist_rad, idx = tree.query(connected_coords, k=1)
    dist_m = dist_rad * earth_radius
    if uniform_radius_m is not None:
        compare_radius = np.full_like(
            dist_m, fill_value=float(uniform_radius_m), dtype=float)
    else:
        compare_radius = poi_data_pd_df.iloc[idx][column_name['poi_radius']].to_numpy(
            dtype=float)
    in_radius = dist_m <= compare_radius
    poi_names = poi_data_pd_df.iloc[idx][column_name['poi_name']].to_numpy()
    result_pd_df['RESULT'] = np.where(in_radius, poi_names, None)
    return result_pd_df


# 訪問頻度算出関数
def visits_ratio(
        trip_pd_df,
        column_name={
            "vin": "MASKED_VIN",
            "model_type": "DISPATCH_MODEL_TYPE",
            "timestamp": "IGOFF_TIMESTAMP",
            "location_name": "RESULT",
        },
        target_col_name="TARGET"
):
    df = trip_pd_df.copy()
    df[column_name["location_name"]] = (
        df[column_name["location_name"]].astype(str).str.strip().replace(
            {"": pd.NA, "None": pd.NA, "none": pd.NA, "NaN": pd.NA, "nan": pd.NA}, regex=False
        )
    )
    df["IS_target"] = df[column_name["location_name"]].notna()
    grouped = df.groupby(column_name["vin"], dropna=False)
    out = grouped.agg(
        TRIP_TOTAL=pd.NamedAgg(column=column_name["vin"], aggfunc="size"),
        TRIP_TARGET=pd.NamedAgg(
            column="IS_target", aggfunc=lambda s: int(s.sum())),
    ).reset_index()
    out["TRIP_OTHER"] = out["TRIP_TOTAL"] - out["TRIP_TARGET"]
    out["TRIP_TARGET_RATIO"] = np.where(out["TRIP_TOTAL"] > 0,
                                        out["TRIP_TARGET"] / out["TRIP_TOTAL"],
                                        0.0)
    out = out.rename(columns={'TRIP_TARGET': 'TRIP_' + target_col_name})
    out = out.rename(
        columns={'TRIP_TARGET_RATIO': 'TRIP_' + target_col_name + '_RATIO'})
    return out


# 訪問回数を1日1回に丸め込む関数
def round_daily_visits(
        trip_pd_df,
        column_name={
            "vin": "MASKED_VIN",
            "model_type": "DISPATCH_MODEL_TYPE",
            "timestamp": "IGOFF_TIMESTAMP",
            "location_name": "RESULT",
        },
        source_timezone="Asia/Tokyo",
        target_timezone="Asia/Tokyo",
):
    df = trip_pd_df.copy()
    df[column_name["location_name"]] = (
        df[column_name["location_name"]].astype(str).str.strip().replace(
            {"": pd.NA, "None": pd.NA, "none": pd.NA, "NaN": pd.NA, "nan": pd.NA}, regex=False
        )
    )
    timestamp = pd.to_datetime(df[column_name["timestamp"]], errors="coerce")
    if source_timezone:
        if timestamp.dt.tz is None:
            timestamp = timestamp.dt.tz_localize(source_timezone)
        else:
            timestamp = timestamp.dt.tz_convert(source_timezone)
    if target_timezone:
        if timestamp.dt.tz is None:
            timestamp = timestamp.dt.tz_localize(target_timezone)
        else:
            timestamp = timestamp.dt.tz_convert(target_timezone)
    df[column_name["timestamp"]] = timestamp.dt.date
    group_cols = [
        column_name["vin"],
        column_name["model_type"],
        column_name["timestamp"],
        column_name["location_name"],
    ]
    out = (
        df.groupby(group_cols, dropna=False, as_index=False).size().rename(
            columns={"size": "COUNT"})
    )
    out["COUNT"] = out["COUNT"].astype(int)
    out = out.sort_values(by=[column_name["vin"], column_name["timestamp"],
                          column_name["location_name"]]).reset_index(drop=True)
    return out


# 訪問日数と停車割合の算出関数
def visits_day_and_ratio(
    df,
    column_name={
        "vin": "MASKED_VIN",
        "model_type": "DISPATCH_MODEL_TYPE",
        "timestamp": "IGOFF_TIMESTAMP",
        "location_name": "RESULT",
    },
    target_col_name="TARGET"
):
    visits_ratio_df = visits_ratio(
        df,
        column_name,
        target_col_name
    )
    visits_days_df = round_daily_visits(
        df,
        column_name,
        source_timezone="Asia/Tokyo",
        target_timezone="Asia/Tokyo",
    )
    visits_days_df = (
        visits_days_df
        .groupby(column_name['vin'], as_index=False)[column_name['location_name']]
        .count()
        .rename(columns={column_name['location_name']: 'TRIP_'+target_col_name+'_COUNT'})
    )
    visits_day_and_ratio_df = (
        visits_days_df.merge(visits_ratio_df[[
                             column_name['vin'], 'TRIP_'+target_col_name+'_RATIO']], on=column_name['vin'])
    )
    return visits_day_and_ratio_df


# トリップ時間の算出関数
def get_duration(
        df,
        column_name={
            "start_timestamp": "IGOFF_TIMESTAMP",
            "end_timestamp": "IGON_TIMESTAMP",
        },
        source_timezone="UTC",
        target_timezone="UTC",
        to_unit='seconds'
):
    # タイムゾーンの適用
    stt_timestamp = pd.to_datetime(df[column_name["start_timestamp"]], errors="coerce")
    end_timestamp = pd.to_datetime(df[column_name["end_timestamp"]], errors="coerce")
    if source_timezone:
        if stt_timestamp.dt.tz is None:
            stt_timestamp = stt_timestamp.dt.tz_localize(source_timezone)
        else:
            stt_timestamp = stt_timestamp.dt.tz_convert(source_timezone)
        if end_timestamp.dt.tz is None:
            end_timestamp = end_timestamp.dt.tz_localize(source_timezone)
        else:
            end_timestamp = end_timestamp.dt.tz_convert(source_timezone)
    if target_timezone:
        if stt_timestamp.dt.tz is None:
            stt_timestamp = stt_timestamp.dt.tz_localize(target_timezone)
        else:
            stt_timestamp = stt_timestamp.dt.tz_convert(target_timezone)
    if target_timezone:
        if end_timestamp.dt.tz is None:
            end_timestamp = end_timestamp.dt.tz_localize(target_timezone)
        else:
            end_timestamp = end_timestamp.dt.tz_convert(target_timezone)
    # 差分を計算
    if to_unit == 'seconds':        
        df["DURATION"] = (end_timestamp - stt_timestamp).dt.total_seconds()
    elif to_unit == 'minutes':        
        df["DURATION"] = (end_timestamp - stt_timestamp).dt.total_seconds() / 60
    elif to_unit == 'hours':        
        df["DURATION"] = (end_timestamp - stt_timestamp).dt.total_seconds() / 3600
    return df
