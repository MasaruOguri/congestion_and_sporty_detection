from typing import Dict, List, Union
import pandas as pd
import numpy as np
from .query_utils import *
from .analysis_utils import *


# 各データで新しく出現したVINを抽出する関数
def get_monthly_new_vins(vin_dict):
    sorted_keys = sorted(vin_dict.keys(), key=lambda k: int(k))
    result = {}
    seen = set()
    for key in sorted_keys:
        current_set = set(vin_dict.get(key, []))
        new_vins = current_set - seen
        result[key] = sorted(new_vins)
        seen |= current_set
    return result


# 前処理用の特徴量テーブルを取得する関数
def get_feature_tables_for_preprocess_vin(
    unique_vin_list,
    snowflake_session,
    data_params={
        "readonly_schema_name": None,
        "schema_name": None,
        "can_table_name": None,
        "circuit_poi_table_name": None,
        "gg_poi_table_name": None,
    },
    can_column_name={
        "vin": "MASKED_VIN",
        "model_type": "DISPATCH_MODEL_TYPE",
        "tripcount": "TRIPCOUNT",
        "timestamp": "GPS_TIMESTAMP",
        "latitude": "LATITUDE",
        "longitude": "LONGITUDE",
        "speed": "SPEED",
        "odometer": "ODOMETER_KM",
    },
    circuit_poi_column_name={
        "poi_name": "NAME",
        "poi_latitude": "LATITUDE",
        "poi_longitude": "LONGITUDE",
        'poi_radius': "RADIUS_M",
    },
    gg_poi_column_name={
        "poi_name": "NAME",
        "poi_latitude": "LATITUDE",
        "poi_longitude": "LONGITUDE",
        'poi_radius': "RADIUS_M",
    },
    poi_radius_m={
        'circuit': 1000,
        'grgarage': 90
    }
):
    # ----------データ抽出パート----------
    # 各VINごとのデータを取得
    print("Data Loading...")
    vin_info_pddf = get_vin_info(
        snowflake_session=snowflake_session,
        data_params={
            "schema_name": data_params["readonly_schema_name"],
            "table_name": data_params["can_table_name"],
        },
        column_name=can_column_name,
        vin_list=unique_vin_list,
    ).to_pandas()
    print(" - VINs information Data Loaded!")
    # 各VIN・各トリップのデータを取得
    trip_records_pddf = get_trip_record(
        snowflake_session=snowflake_session,
        data_params={
            "schema_name": data_params["readonly_schema_name"],
            "table_name": data_params["can_table_name"],
        },
        column_name=can_column_name,
        vin_list=unique_vin_list,
    ).to_pandas()
    print(" - Trip Records Data Loaded!")
    # 国内サーキットデータを取得
    circuit_list_pddf = get_snowflake_table(
        snowflake_session=snowflake_session,
        data_params={
            "schema_name": data_params["schema_name"],
            "table_name": data_params["circuit_poi_table_name"],
        },
    ).to_pandas()
    print(" - Circuit POI Data Loaded!")
    # GR Garageデータを取得
    gg_list_pddf = get_snowflake_table(
        snowflake_session=snowflake_session,
        data_params={
            "schema_name": data_params["schema_name"],
            "table_name": data_params["gg_poi_table_name"],
        },
    ).to_pandas()
    print(" - GR Garage POI Data Loaded!")
    # ----------走行判定パート----------
    # 国内サーキットIG-OFF判定
    print("Drivings/Visits Analysis...")
    trip_circuit_pddf = judge_vehicle_in_radius(
        trip_records_pddf,
        circuit_list_pddf,
        column_name={
            'latitude': 'IGOFF_LATITUDE',
            'longitude': 'IGOFF_LONGITUDE',
            'poi_name': circuit_poi_column_name['poi_name'],
            'poi_latitude': circuit_poi_column_name['poi_latitude'],
            'poi_longitude': circuit_poi_column_name['poi_longitude'],
            'poi_radius': circuit_poi_column_name['poi_radius']
        },
        uniform_radius_m=poi_radius_m['circuit']
    )
    print(" - Circuit Drivings Analysis Done!")
    # GR Garage IG-OFF判定
    trip_gg_pddf = judge_vehicle_in_radius(
        trip_records_pddf,
        gg_list_pddf,
        column_name={
            'latitude': 'IGOFF_LATITUDE',
            'longitude': 'IGOFF_LONGITUDE',
            'poi_name': gg_poi_column_name['poi_name'],
            'poi_latitude': gg_poi_column_name['poi_latitude'],
            'poi_longitude': gg_poi_column_name['poi_longitude'],
            'poi_radius': gg_poi_column_name['poi_radius']
        },
        uniform_radius_m=poi_radius_m['grgarage']
    )
    print(" - GR Garage Visits Analysis Done!")
    # ----------訪問/停車傾向の算出パート----------
    # 国内サーキットの訪問日数と停車割合
    print("Computing Drivings/Visits Ratio...")
    trip_circuit_day_and_ratio_pddf = visits_day_and_ratio(
        trip_circuit_pddf,
        column_name={
            "vin": can_column_name["vin"],
            "model_type": can_column_name["model_type"],
            "timestamp": "IGOFF_TIMESTAMP",
            "location_name": "RESULT",
        },
        target_col_name="CIRCUIT"
    )
    print(" - Circuit Drivings Ratio Done!")
    # GR Garageの訪問日数と停車割合
    trip_gg_day_and_ratio_pddf = visits_day_and_ratio(
        trip_gg_pddf,
        column_name={
            "vin": can_column_name["vin"],
            "model_type": can_column_name["model_type"],
            "timestamp": "IGOFF_TIMESTAMP",
            "location_name": "RESULT",
        },
        target_col_name="GG"
    )
    print(" - GR Garage Visits Ratio Done!")
    return {
        "vin_info_pddf": vin_info_pddf,
        "trip_circuit_pddf": trip_circuit_day_and_ratio_pddf,
        "trip_grgarage_pddf": trip_gg_day_and_ratio_pddf,
    }


# 閾値でVINを絞り込む関数
def filter_vin(
    df_dict,
    column_name={
        "vin": "MASKED_VIN",
    },
    vin_list=[],
    thresholds={
        'tripcount': 10,
        'odometer': 100,
        'circuit_days': 10,
        'circuit_ratio': 0.5,
        'grgarage_days': 10,
        'grgarage_ratio': 0.5
    }
):
    filered_info_vin = df_dict["vin_info_pddf"][
        (df_dict["vin_info_pddf"]['TRIPCOUNT_DIFF'] <= thresholds["tripcount"]) |
        (df_dict["vin_info_pddf"]['ODOMETER_DIFF'] <= thresholds["odometer"])
    ][column_name["vin"]].to_list()
    filtered_circuit_vin = df_dict["trip_circuit_pddf"][
        (df_dict["trip_circuit_pddf"]['TRIP_CIRCUIT_COUNT'] >= thresholds["circuit_days"]) |
        (df_dict["trip_circuit_pddf"]['TRIP_CIRCUIT_RATIO']
         >= thresholds["circuit_ratio"])
    ][column_name["vin"]].to_list()
    filtered_gg_vin = df_dict["trip_grgarage_pddf"][
        (df_dict["trip_grgarage_pddf"]['TRIP_GG_COUNT'] >= thresholds["grgarage_days"]) |
        (df_dict["trip_grgarage_pddf"]['TRIP_GG_RATIO']
         >= thresholds["grgarage_ratio"])
    ][column_name["vin"]].to_list()
    remove_vin_list = set(filered_info_vin) | set(
        filtered_circuit_vin) | set(filtered_gg_vin)
    target_vin_list = [x for x in vin_list if x not in remove_vin_list]
    print(f'Number of VINs: {len(vin_list)} --> {len(target_vin_list)}')
    return target_vin_list


# テーブルを結合する関数
def concat_table(
    df_dict,
    column_name={
        "vin": "MASKED_VIN",
    },
):
    yyyymm_key_list = list(df_dict.keys())
    tables_key_list = list(list(df_dict.items())[0][1].keys())
    df_concat_dict = {}
    for table_key in tables_key_list:
        for yyyymm_key in yyyymm_key_list:
            if yyyymm_key == yyyymm_key_list[0]:
                df_concat = df_dict[yyyymm_key][table_key]
            else:
                df_concat = pd.concat(
                    [df_concat, df_dict[yyyymm_key][table_key]],
                    axis=0,
                    ignore_index=True
                )
            df_concat_dict[table_key] = df_concat
    return df_concat_dict
