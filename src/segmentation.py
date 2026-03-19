from typing import Dict, List, Union
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from .query_utils import *
from .analysis_utils import *
from .generate_feature_table import *


# セグメンテーション用の特徴量テーブルを取得する関数
def get_feature_tables_for_segmentation(
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
    },
    thresholds={
        'min_circuit_speed': 50,
        'min_grgarage_stay_seconds':30*60
    }
):
    # ----------データ取得パート----------
    # 各VIN・各トリップごとのIG-OFFデータ
    print("Data Loading...")
    trip_records_pddf = get_trip_record(
        snowflake_session=snowflake_session,
        data_params={
            "schema_name": data_params["readonly_schema_name"],
            "table_name": data_params["can_table_name"],
            },
        column_name = can_column_name,
        vin_list=unique_vin_list,
    ).to_pandas()
    print(" - Trip Records Loaded!")
    # 各VIN・各トリップごとの最高速度データ
    trip_maxspeed_pddf = get_maxspeed_record(
        snowflake_session=snowflake_session,
        data_params={
            "schema_name": data_params["readonly_schema_name"],
            "table_name": data_params["can_table_name"],
            },
        column_name=can_column_name,
        vin_list=unique_vin_list,
    ).to_pandas()
    print(" - Max Speed Records Loaded!")
    # 各VIN・各トリップごとの走行距離データ
    trip_distance_pddf = get_trip_distance(
        snowflake_session=snowflake_session,
        data_params={
            "schema_name": data_params["readonly_schema_name"],
            "table_name": data_params["can_table_name"],
            },
        column_name=can_column_name,
        vin_list=unique_vin_list,
    ).to_pandas()
    print(" - Trip Distance Records Loaded!")
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
    # 国内サーキット走行判定
    print("Drivings/Visits Analysis...")
    drive_circuit_pddf = judge_vehicle_in_radius(
        trip_maxspeed_pddf, 
        circuit_list_pddf,
        column_name = {
            'latitude': 'MAX_SPEED_LATITUDE',
            'longitude': 'MAX_SPEED_LONGITUDE',
            'poi_name': circuit_poi_column_name['poi_name'],
            'poi_latitude': circuit_poi_column_name['poi_latitude'],
            'poi_longitude': circuit_poi_column_name['poi_longitude'],
            'poi_radius': circuit_poi_column_name['poi_radius']
        },
        uniform_radius_m=poi_radius_m['circuit']
        )
    print(" - Circuit Driving Analysis Done!")
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
    # ----------特徴テーブル作成パート----------
    # サーキット走行テーブル
    print("Feature Table Generation...")
    driving_circuit_table_pddf = generate_driving_table(
        drive_circuit_pddf,
        column_name={
            "vin": can_column_name["vin"],
            "model_type": can_column_name["model_type"],
            "timestamp":"MAX_SPEED_TIMESTAMP",
            "location_name": "RESULT",
            "peak_speed": "MAX_SPEED",
        },
        speed_threshold=thresholds["min_circuit_speed"],
    )
    print(" - Circuit Driving Table Generated!")
    # GR Garage来店テーブル
    visit_grgarage_table_pddf = generate_visit_table(
        trip_gg_pddf,
        column_name={
            "vin": can_column_name["vin"],
            "model_type": can_column_name["model_type"],
            "start_timestamp": "IGOFF_TIMESTAMP",
            "end_timestamp": "IGON_TIMESTAMP",
            "location_name": "RESULT",
        },
        time_threshold=thresholds["min_grgarage_stay_seconds"],
    )
    print(" - GR Garage Visit Table Generated!")
    # 全トリップにおける最長走行距離テーブル
    maxdistance_table_pddf = generate_max_distance_table(
        trip_distance_pddf,
        column_name={
            "vin": can_column_name["vin"],
            "model_type": can_column_name["model_type"],
            "distance": "TRIP_DISTANCE_KM",
        },
    )
    print(" - Max Distance Table Generated!")
    # 全トリップにおける最高車速テーブル
    maxspeed_table_pddf = generate_max_speed_table(
        trip_maxspeed_pddf,
        column_name={
            "vin": can_column_name["vin"],
            "model_type": can_column_name["model_type"],
            "speed": "MAX_SPEED",
        },
    )
    print(" - Max Speed Table Generated!")
    # 結合
    table_pddf = driving_circuit_table_pddf.merge(visit_grgarage_table_pddf, on=[can_column_name["vin"], can_column_name["model_type"]], how="outer")
    table_pddf = table_pddf.merge(maxdistance_table_pddf, on=[can_column_name["vin"], can_column_name["model_type"]], how="outer")
    table_pddf = table_pddf.merge(maxspeed_table_pddf, on=[can_column_name["vin"], can_column_name["model_type"]], how="outer")
    table_pddf = pd.DataFrame(unique_vin_list, columns=[can_column_name["vin"]]).merge(table_pddf, on=[can_column_name["vin"]], how="outer")
    # カラム名を変更
    table_pddf = table_pddf.rename(columns={
        'DRIVING_COUNT': 'CIRCUIT_DRIVING_COUNT',
        'DRIVING_MAX_SPEED': 'CIRCUIT_DRIVING_MAX_SPEED',
        'VISIT_COUNT': 'GRGARAGE_VISIT_COUNT',
        })
    print(" - Feature Table Generated!")
    return table_pddf


# ルールベースのセグメンテーションを行う関数
def rule_based_segmentation(
    df, 
    rule_def,
    segment_names=["High-Amateur", "Middle", "Light", "Entry"],
    default_segment_name="Entry",
    segment_column_name="SEGMENT",
    ):
    result = df.copy()
    result[segment_column_name] = np.select(
        condlist=rule_def(df),
        choicelist=segment_names,
        default=default_segment_name  
    )
    result[segment_column_name] = pd.Categorical(result[segment_column_name], categories=segment_names, ordered=True)
    print(f'Segmentation result (Total Sample:  {len(result)}')
    print(f'{result["SEGMENT"].value_counts()}')
    return result


# セグメンテーション結果を集計する関数（年月ごと）
def count_segmentation_result(
    df_dict,
    column_name={
        'vin': 'MASKED_VIN',
        'segment': 'SEGMENT'
    }
    ):
    N_ser = pd.Series({year: len(df) for year, df in df_dict.items()}, name='N').sort_index()
    counts_df = pd.DataFrame({
        year: (
            df
            .dropna(subset=[column_name['segment']])     
            .groupby(column_name['segment'])[column_name['vin']]
            .nunique()              
        )for year, df in df_dict.items()
    }).T.fillna(0).astype(int)
    pct_df = (counts_df.div(N_ser, axis=0) * 100).round(1)
    pct_df.columns = [f'{c}(%)' for c in pct_df.columns] 
    return N_ser, counts_df, pct_df


# セグメンテーション結果を可視化する関数（年月ごと）
def plot_segmentation_result(
    df_dict,
    y_100pct=True,
    figsize=(10, 5),
    color=['C3', 'C2', 'C1', 'C0'],
    column_name={
        'vin': 'MASKED_VIN',
        'segment': 'SEGMENT'
    }
):
    N_pdser, counts_pddf, pct_pddf = count_segmentation_result(df_dict)
    if y_100pct:
        df = pct_pddf
    else:
        df = counts_pddf
    df.index.name = 'YYYYMM'
    idx = df.index
    seg_cols = df.columns
    idx_dt = pd.to_datetime(idx.astype(str), format="%Y%m", errors="coerce")
    use_dt = not idx_dt.isna().any()
    x = np.arange(len(df))
    if use_dt:
        xtick_labels = idx_dt.strftime("%Y-%m") 
    else:
        xtick_labels = idx.astype(str)      
    fig, ax = plt.subplots(figsize=figsize)
    bottom = np.zeros(len(df))
    for seg in seg_cols[::-1]:
        ax.bar(
            x, df[seg].values,
            bottom=bottom,
            label=seg.replace("(%)", ""),
            width=1/len(seg_cols)*2,
            color=color[seg_cols.get_loc(seg)],
        )
        bottom += df[seg].values
    ax.set_xticks(x)
    ax.set_xticklabels(xtick_labels, rotation=45, ha="right")
    ax.set_xlabel("Year-Month")
    ax.grid(axis="y", linestyle="--")
    if y_100pct:
        ax.set_ylim(0, 100)
        ax.set_ylabel("%")
    else:
        ax.set_ylabel("Number of Vehicles")
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], frameon=False,
              loc='upper center', ncols=len(seg_cols), bbox_to_anchor=(0.5, 1.1))
    plt.tight_layout()
    plt.show()


# セグメンテーション結果を集計する関数（モデルごと）
def count_segmentation_result_by_modeltype(
    df_dict,
    column_name={
        'vin': 'MASKED_VIN',
        'modeltype': 'DISPATCH_MODEL_TYPE',
        'segment': 'SEGMENT'
    }
    ):
    df_concat = pd.concat(
        [df.assign(YYYYMM=year) for year, df in df_dict.items()],
        axis=0,
        ignore_index=True
    )
    N_ser = df_concat[column_name['modeltype']].value_counts(dropna=True)
    N_ser.name = 'N'
    counts_df = pd.crosstab(
        index=df_concat[column_name['modeltype']],
        columns=df_concat[column_name['segment']],
        dropna=True 
    )
    pct_df = (counts_df.div(N_ser, axis=0) * 100).round(2)
    pct_df.columns = [f'{c}(%)' for c in pct_df.columns] 
    return N_ser, counts_df, pct_df


# セグメンテーション結果を可視化する関数（モデルごと）
def plot_segments_by_modeltype(
    df_dict,
    x_100pct=True,
    N_col="N",
    figsize=(10, 5),
    legend_h=1.1,
    # 全系列（列）名の固定順序を指定。ここに想定される全セグメント名を列挙してください
    all_segments=None,
    # セグメントごとの固定色割り当て辞書
    color_map=None,
    column_name={
        'vin': 'MASKED_VIN',
        'modeltype': 'DISPATCH_MODEL_TYPE',
        'segment': 'SEGMENT'
    }
):
    N_pdser, counts_pddf, pct_pddf = count_segmentation_result_by_modeltype(df_dict)
    if x_100pct:
        df = pd.concat([N_pdser, pct_pddf], axis=1)
    else:
        df = pd.concat([N_pdser, counts_pddf], axis=1)
    df = df.drop(columns=[N_col])
    df.columns = [c.replace("(%)", "") for c in df.columns]
    # all_segmentsが未指定の場合はdf.columnsをソートしたものを採用（非推奨だがフォールバック用）
    if all_segments is None:
        all_segments = sorted(df.columns.tolist())
    # color_mapが未指定ならall_segmentsに対応するデフォルト色を割り当て
    if color_map is None:
        default_colors = ['C0', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9']
        color_map = {seg: default_colors[i % len(default_colors)] for i, seg in enumerate(all_segments)}
    # データに存在する列だけ抽出し、all_segmentsの順序で並び替え
    plot_columns = [seg for seg in all_segments if seg in df.columns]
    df_plot = df[plot_columns]
    # 色もplot_columnsの順に並び替え
    colors = [color_map[seg] for seg in plot_columns]
    df_plot[::-1].plot(kind='barh', stacked=True, figsize=figsize, color=colors)
    plt.grid(axis='x', linestyle='--', alpha=0.5)
    plt.legend(loc='upper center', ncols=len(plot_columns), bbox_to_anchor=(0.5, legend_h), frameon=False)
    plt.ylabel('MODEL TYPE')
    if x_100pct:
        plt.xlim(0, 100)
        plt.xlabel('%')
    else:
        plt.xlabel('Number of Vehicles')
    plt.tight_layout()
    plt.show()


# テーブルを結合する関数
def concat_table(
    df_dict,
    column_name={
        "vin": "MASKED_VIN",
    },
):
    return pd.concat(
        [df.assign(YYYYMM=year) for year, df in df_dict.items()],
        axis=0,
        ignore_index=True
    )
