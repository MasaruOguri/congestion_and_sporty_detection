from typing import Dict, List, Union
import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from .analysis_utils import *


# 走行特徴テーブル作成関数
def generate_driving_table(
    df,
    column_name={
        "vin": "MASKED_VIN",
        "model_type": "DISPATCH_MODEL_TYPE",
        "timestamp":"PEAK_SPEED_TIMESTAMP",
        "location_name": "RESULT",
        "peak_speed": "PEAK_SPEED",
    },
    speed_threshold=50,
    source_timezone="Asia/Tokyo",
    target_timezone="Asia/Tokyo",
):
    df = df[df[column_name['peak_speed']] >= speed_threshold]
    daily_df = round_daily_visits(
        df,
        column_name,
        source_timezone,
        target_timezone,
    )
    daily_counts_df = (
        daily_df
        .groupby([
            column_name['vin'], 
            column_name['model_type'], 
            ], 
            as_index=False)[column_name['location_name']]
        .count()
        .rename(columns={column_name['location_name']: 'DRIVING_COUNT'})
    )
    max_speed_speed = (
        df[df[column_name['location_name']].notnull()]
        .groupby(column_name['vin'], as_index=False)[column_name['peak_speed']]
        .max()
        .rename(columns={column_name['peak_speed']: 'DRIVING_MAX_SPEED'})
    )
    out = daily_counts_df.merge(max_speed_speed, on=column_name['vin'], how='left')
    return out


# 訪問特徴テーブル作成関数
def generate_visit_table(
    df,
    column_name={
        "vin": "MASKED_VIN",
        "model_type": "DISPATCH_MODEL_TYPE",
        "start_timestamp": "IGOFF_TIMESTAMP",
        "end_timestamp": "IGON_TIMESTAMP",
        "location_name": "RESULT",
    },
    time_threshold=50,
    source_timezone="Asia/Tokyo",
    target_timezone="Asia/Tokyo",
):
    df = get_duration(
        df,
        column_name,
        source_timezone,
        target_timezone,
        to_unit='seconds'
    )
    df = df[df['DURATION'] >= time_threshold]
    daily_df = round_daily_visits(
        df,
        {
            "vin": column_name["vin"],
            "model_type": column_name["model_type"],
            "timestamp":column_name["start_timestamp"],
            "location_name": column_name["location_name"],
        },
        source_timezone,
        target_timezone,
    )
    daily_counts_df = (
        daily_df
        .groupby([
            column_name['vin'], 
            column_name['model_type'], 
            ], 
            as_index=False)[column_name['location_name']]
        .count()
        .rename(columns={column_name['location_name']: 'VISIT_COUNT'})
    )
    return daily_counts_df


# 最長走行距離テーブル作成関数
def generate_max_distance_table(
    df,
    column_name={
        "vin": "MASKED_VIN",
        "model_type": "DISPATCH_MODEL_TYPE",
        "distance": "TRIP_DISTANCE_KM",
    },
    distance_threshold=5,
):
    out = (
            df.groupby([
            column_name['vin'], 
            column_name['model_type'], 
            ], 
            as_index=False)[column_name['distance']]
            .max()
            .rename(columns={column_name['distance']: 'MAX_DISTANCE'})
        )
    return out


# 最高速度トリップのテーブル作成関数
def generate_max_speed_table(
    df,
    column_name={
        "vin": "MASKED_VIN",
        "model_type": "DISPATCH_MODEL_TYPE",
        "speed_max": "SPEED_MAX",
    },
):
    out = (
            df.groupby([
            column_name['vin'], 
            column_name['model_type'], 
            ], 
            as_index=False)[column_name['speed']]
            .max()
            .rename(columns={column_name['speed']: 'MAX_SPEED'})
        )
    return out