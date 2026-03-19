import warnings
from src import snowflake_oauth
from snowflake.snowpark.session import Session
warnings.simplefilter("ignore")


# Snowflakeセッション生成関数
def get_snowflake_session(snowflake_params):
    acces_token = snowflake_oauth.access_token()
    params = {
        "account": snowflake_params["account"],
        "user": snowflake_params["user"],
        "database": snowflake_params["database"],
        "token": acces_token,
        "authenticator": "oauth",
    }
    return Session.builder.configs(params).create()


# テーブル取得関数
def get_snowflake_table(
    snowflake_session,
    data_params,
):
    query = f"""
        SELECT *
        FROM "{data_params['schema_name']}"."{data_params['table_name']}"
    """
    return snowflake_session.sql(query)


# VINのフィルタクエリ生成関数
def _get_vin_filter(vin_list, vin_column_name, call_type="WHERE"):
    def _escape_sql_literal(value: str) -> str:
        return value.replace("'", "''")
    normalized = []
    if vin_list is None or vin_list == "":
        normalized = []
    elif isinstance(vin_list, str):
        normalized = [vin_list]
    else:
        normalized = [
            mt for mt in vin_list if mt is not None and str(mt) != ""]
    if len(normalized) == 0:
        vin_filter = ""
    elif len(normalized) == 1:
        only = _escape_sql_literal(normalized[0])
        vin_filter = f"{call_type} {vin_column_name} = '{only}'"
    else:
        escaped = [f"'{_escape_sql_literal(mt)}'" for mt in normalized]
        in_list = ", ".join(escaped)
        vin_filter = f"{call_type} {vin_column_name} IN ({in_list})"
    return vin_filter


# ユニークなVINとその車両型式を取得する関数
def get_unique_vin_and_model_type(
    snowflake_session,
    data_params,
    column_name={
        "vin": "MASKED_VIN",
        "model_type": "DISPATCH_MODEL_TYPE",
    }
):
    query = f"""
        SELECT DISTINCT
            {column_name['vin']},
            {column_name['model_type']}
        FROM "{data_params['schema_name']}"."{data_params['table_name']}"
    """
    return snowflake_session.sql(query)


# 各VINの走行情報（車両型式・走行距離・トリップ数）を取得する関数
def get_vin_info(
    snowflake_session,
    data_params,
    timestamp_style="YYYY-MM-DD HH24:MI:SS.FF3",
    column_name={
        "vin": "MASKED_VIN",
        "model_type": "DISPATCH_MODEL_TYPE",
        "tripcount": "TRIPCOUNT",
        "timestamp": "GPS_TIMESTAMP",
        "odometer": "ODOMETER_KM"
    },
    source_timezone="UTC",
    target_timezone="Asia/Tokyo",
    vin_list=None,
    value_range={
        "tripcount": [0, 65534],
        "odometer": [0, 999999]
    }
):
    vin_filter = _get_vin_filter(vin_list, column_name['vin'])
    query = f"""
        WITH BASE AS (
            SELECT
                {column_name['vin']},
                {column_name['model_type']},
                {column_name['tripcount']},
                {column_name['odometer']},
                CONVERT_TIMEZONE(
                    '{source_timezone}',
                    '{target_timezone}',
                    TO_TIMESTAMP({column_name['timestamp']}, '{timestamp_style}')
                ) AS TS_UTC
            FROM "{data_params['schema_name']}"."{data_params['table_name']}"
            {vin_filter}
                AND {column_name['tripcount']} BETWEEN {value_range['tripcount'][0]} AND {value_range['tripcount'][1]}
                AND {column_name['odometer']} BETWEEN {value_range['odometer'][0]} AND {value_range['odometer'][1]}
        ),
        VIN_BOUNDS AS (
            SELECT
                {column_name['vin']},
                {column_name['model_type']},
                FIRST_VALUE({column_name['tripcount']}) OVER (
                    PARTITION BY {column_name['vin']}
                    ORDER BY {column_name['tripcount']} ASC, TS_UTC ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS FIRST_TRIPCOUNT,
                FIRST_VALUE({column_name['odometer']}) OVER (
                    PARTITION BY {column_name['vin']}
                    ORDER BY {column_name['tripcount']} ASC, TS_UTC ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS FIRST_ODOMETER_KM,
                LAST_VALUE({column_name['tripcount']}) OVER (
                    PARTITION BY {column_name['vin']}
                    ORDER BY {column_name['tripcount']} ASC, TS_UTC ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS LAST_TRIPCOUNT,
                LAST_VALUE({column_name['odometer']}) OVER (
                    PARTITION BY {column_name['vin']}
                    ORDER BY {column_name['tripcount']} ASC, TS_UTC ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS LAST_ODOMETER_KM
            FROM BASE
        ),
        VIN_DIFF AS (
            SELECT DISTINCT
                {column_name['vin']},
                {column_name['model_type']},
                GREATEST(LAST_TRIPCOUNT - FIRST_TRIPCOUNT, 0) AS TRIPCOUNT_DIFF,
                GREATEST(LAST_ODOMETER_KM - FIRST_ODOMETER_KM, 0) AS ODOMETER_DIFF
            FROM VIN_BOUNDS
        )
        SELECT
            {column_name['vin']},
            {column_name['model_type']},
            TRIPCOUNT_DIFF,
            ODOMETER_DIFF
        FROM VIN_DIFF
        ORDER BY {column_name['vin']}
    """
    return snowflake_session.sql(query)


# 各VINのトリップ情報（IGOFFの緯度経度や時刻）を取得する関数
def get_trip_record(
    snowflake_session,
    data_params,
    timestamp_style="YYYY-MM-DD HH24:MI:SS.FF3",
    column_name={
        "vin": "MASKED_VIN",
        "model_type": "DISPATCH_MODEL_TYPE",
        "tripcount": "TRIPCOUNT",
        "timestamp": "GPS_TIMESTAMP",
        "latitude": "LATITUDE",
        "longitude": "LONGITUDE",
    },
    source_timezone="UTC",
    target_timezone="Asia/Tokyo",
    vin_list=None,
    value_range={
        "tripcount": [0, 65534],
    }
):
    vin_filter = _get_vin_filter(vin_list, column_name['vin'])
    query = f"""
        WITH SRC AS (
            SELECT
                {column_name['vin']},
                {column_name['model_type']},
                {column_name['tripcount']},
                {column_name['latitude']},
                {column_name['longitude']},
                CONVERT_TIMEZONE(
                    '{source_timezone}',
                    '{target_timezone}',
                    TO_TIMESTAMP({column_name['timestamp']}, '{timestamp_style}')
                ) AS TS_UTC
            FROM "{data_params['schema_name']}"."{data_params['table_name']}"
            {vin_filter}
                AND {column_name['tripcount']} BETWEEN {value_range['tripcount'][0]} AND {value_range['tripcount'][1]} 
        ),
        BASE AS (
            SELECT
                {column_name['vin']},
                {column_name['model_type']},
                LAG({column_name['latitude']})  OVER (
                    PARTITION BY {column_name['vin']}
                    ORDER BY {column_name['tripcount']}, TS_UTC
                ) AS IGOFF_LATITUDE,
                LAG({column_name['longitude']}) OVER (
                    PARTITION BY {column_name['vin']}
                    ORDER BY {column_name['tripcount']}, TS_UTC
                ) AS IGOFF_LONGITUDE,
                LAG(TS_UTC) OVER (
                    PARTITION BY {column_name['vin']}
                    ORDER BY {column_name['tripcount']}, TS_UTC
                ) AS IGOFF_TIMESTAMP,
                TS_UTC AS IGON_TIMESTAMP,
                {column_name['tripcount']}
            FROM SRC
            QUALIFY {column_name['tripcount']} > LAG({column_name['tripcount']}) OVER (
                PARTITION BY {column_name['vin']}
                ORDER BY {column_name['tripcount']}, TS_UTC
            )
        )
        SELECT
            {column_name['vin']},
            {column_name['model_type']},
            IGOFF_LATITUDE,
            IGOFF_LONGITUDE,
            IGOFF_TIMESTAMP,
            IGON_TIMESTAMP
        FROM BASE
    """
    return snowflake_session.sql(query)


# トリップ内の最高車速レコードを取得する関数
def get_maxspeed_record(
    snowflake_session,
    data_params,
    timestamp_style="YYYY-MM-DD HH24:MI:SS.FF3",
    column_name={
        "vin": "MASKED_VIN",
        "model_type": "DISPATCH_MODEL_TYPE",
        "tripcount": "TRIPCOUNT",
        "timestamp": "GPS_TIMESTAMP",
        "latitude": "LATITUDE",
        "longitude": "LONGITUDE",
        "speed": "SPEED"
    },
    source_timezone="UTC",
    target_timezone="Asia/Tokyo",
    vin_list=None,
    value_range={
        "tripcount": [0, 65534],
        "speed": [0, 655.35]
    }
):
    vin_filter = _get_vin_filter(vin_list, column_name['vin'], call_type="AND")
    query = f"""
        WITH BASE AS (
            SELECT
                {column_name['vin']},
                {column_name['model_type']},
                {column_name['tripcount']},
                CONVERT_TIMEZONE(
                    '{source_timezone}',
                    '{target_timezone}',
                    TO_TIMESTAMP({column_name['timestamp']}, '{timestamp_style}')
                ) AS TS_UTC,
                {column_name['latitude']}  AS {column_name['latitude']},
                {column_name['longitude']} AS {column_name['longitude']},
                {column_name['speed']}     AS {column_name['speed']}
            FROM "{data_params['schema_name']}"."{data_params['table_name']}"
            WHERE {column_name['tripcount']} BETWEEN {value_range['tripcount'][0]} AND {value_range['tripcount'][1]} 
                AND {column_name['speed']} BETWEEN {value_range['speed'][0]} AND {value_range['speed'][1]} 
                {vin_filter}
        ),
        MAX_SPEED_PER_TRIP AS (
            SELECT
                {column_name['vin']},
                {column_name['model_type']},
                {column_name['tripcount']},
                TS_UTC AS MAX_SPEED_TIMESTAMP,
                {column_name['latitude']}  AS MAX_SPEED_LATITUDE,
                {column_name['longitude']} AS MAX_SPEED_LONGITUDE,
                {column_name['speed']}     AS MAX_SPEED
            FROM BASE
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY {column_name['vin']}, {column_name['tripcount']}
                ORDER BY {column_name['speed']} DESC, TS_UTC ASC
            ) = 1
        )
        SELECT
            {column_name['vin']},
            {column_name['model_type']},
            {column_name['tripcount']},
            MAX_SPEED_TIMESTAMP,
            MAX_SPEED_LATITUDE,
            MAX_SPEED_LONGITUDE,
            MAX_SPEED
        FROM MAX_SPEED_PER_TRIP
    """
    return snowflake_session.sql(query)


def get_trip_distance(
    snowflake_session,
    data_params,
    timestamp_style="YYYY-MM-DD HH24:MI:SS.FF3",
    column_name={
            "vin": "MASKED_VIN",
            "model_type": "DISPATCH_MODEL_TYPE",
            "tripcount": "TRIPCOUNT",
            "timestamp": "GPS_TIMESTAMP",
            "latitude": "LATITUDE",
            "longitude": "LONGITUDE",
            "odometer": "ODOMETER_KM" 
        },
    source_timezone="UTC",  
    target_timezone="Asia/Tokyo",         
    vin_list=None,
    value_range={
        "tripcount": [0, 65534],
        "odometer": [0, 999999]
    }
):
    vin_filter = _get_vin_filter(vin_list, column_name['vin'])
    query = f"""
        WITH BASE AS (
            SELECT
                {column_name['vin']},
                {column_name['model_type']},
                {column_name['tripcount']},
                CONVERT_TIMEZONE(
                    '{source_timezone}',
                    '{target_timezone}',
                    TO_TIMESTAMP({column_name['timestamp']}, '{timestamp_style}')
                ) AS TS_UTC,
                {column_name['odometer']} AS {column_name['odometer']}
            FROM "{data_params['schema_name']}"."{data_params['table_name']}"          
            {vin_filter}  
                AND {column_name['tripcount']} BETWEEN {value_range['tripcount'][0]} AND {value_range['tripcount'][1]}
                AND {column_name['odometer']} BETWEEN {value_range['odometer'][0]} AND {value_range['odometer'][1]}
        ),
        TRIP_BOUNDS AS (
            SELECT
                {column_name['vin']},
                {column_name['model_type']},
                {column_name['tripcount']},
                FIRST_VALUE({column_name['odometer']}) OVER (
                    PARTITION BY {column_name['vin']}, {column_name['tripcount']}
                    ORDER BY TS_UTC ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS IGON_ODOMETER_KM,
                FIRST_VALUE(TS_UTC) OVER (
                    PARTITION BY {column_name['vin']}, {column_name['tripcount']}
                    ORDER BY TS_UTC ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS IGON_TIMESTAMP,
                LAST_VALUE({column_name['odometer']}) OVER (
                    PARTITION BY {column_name['vin']}, {column_name['tripcount']}
                    ORDER BY TS_UTC ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS IGOFF_ODOMETER_KM,
                LAST_VALUE(TS_UTC) OVER (
                    PARTITION BY {column_name['vin']}, {column_name['tripcount']}
                    ORDER BY TS_UTC ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS IGOFF_TIMESTAMP
            FROM BASE
        ),
        TRIP_DISTANCE AS (
            SELECT DISTINCT
                {column_name['vin']},
                {column_name['model_type']},
                {column_name['tripcount']},
                IGON_TIMESTAMP,
                IGOFF_TIMESTAMP,
                IGON_ODOMETER_KM,
                IGOFF_ODOMETER_KM,
                GREATEST(IGOFF_ODOMETER_KM - IGON_ODOMETER_KM, 0) AS TRIP_DISTANCE_KM
            FROM TRIP_BOUNDS
        )
        SELECT
            {column_name['vin']},
            {column_name['model_type']},
            {column_name['tripcount']},
            IGON_TIMESTAMP,
            IGOFF_TIMESTAMP,
            IGON_ODOMETER_KM,
            IGOFF_ODOMETER_KM,
            TRIP_DISTANCE_KM
        FROM TRIP_DISTANCE
        ORDER BY {column_name['vin']}, {column_name['tripcount']}
    """
    return snowflake_session.sql(query)
