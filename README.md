# driver-segmentation-with-can-data-analytics

※ このソースコードはTMC-NRVANA内のDatabricks環境で実行してください。

## はじめにやること
（内容作成中）

## リポジトリ構成 📁

```
driver-segmentation-with-can-data-analytics/
├── data/
│   ├── CIRCUIT_MODE_AVAILABLE_POI_202506.csv # 可視化ユーティリティ
│   └── JP_GR_GARAGE_LIST.csv         　　　　 # データ取得・前処理クエリユーティリティ
├── notebook/
│   ├── 00_get_max_speed_table.ipynb   　　　　# 動作テスト用ノートブック
│   └── 01_select_target_vin.ipynb   　　　  　# 手順1) 分析対象VINの絞り込み
│   └── 02_driver_segmentation.ipynb   　　　　# 手順2) ドライバーのセグメンテーション
├── src/
│   ├── analysis_utils.py                     # 分析処理ユーティリティ
│   ├── plot_utils.py                　　　　  # 可視化ユーティリティ
│   ├── query_utils.py               　　　　  # データ取得・前処理クエリユーティリティ
│   ├── segmentation.py              　　　　  # セグメンテーション関数群
│   ├── select_target_vin.py         　　　　  # VIN絞り込み関数群
│   ├── snowflake_oauth.py         　　　　　  # NRVANA初期設定時に生成したものを配置
│   └── utils.py         　            　　　  # 汎用ユーティリティ
├── .env                            　　　　   # 環境変数を記載
├── .gitignore                       　　　　  # 無視するファイル/フォルダの定義
├── README.md                       　　　　   # プロジェクトの概要・説明
├── requirements.txt                   　　　　# Python依存ライブラリ
└── token.pickle                      　　　　 # Snowflake認証/トークン
```

### 各ディレクトリ/ファイルの役割

- **data/**  
  車両の訪問先や走行場所の判定に用いるデータが格納されています。Snowflakeの任意のスキーマにアップロードしてください。
  その後、アップロード情報を`.env`に設定してください。
  （配置したスキーマ名を`SNOWFLAKE_SCHEMA`に、アップロード時に指定したテーブル名を`SNOWFLAKE_TABLE_CIRCUITMODE_AVAILABLE_POI`と`SNOWFLAKE_TABLE_TMI_GRGARAGE_POI`に設定してください。）

- **notebook/**  
  実行用の Jupyter Notebook を配置します。
  `00_get_max_speed_table.ipynb` は、データから最高速度テーブルを作成するサンプルノートブックです。

- **src/**  
  再利用可能なユーティリティを格納します。
  `query_utils.py`の使用には`snowflake_oauth.py` が必要です。`snowflake_oauth.py` はNRVANAお知らせページの「Snowflake接続用ファイル」からダウンロードして配置してください。

- **.env**  
  接続文字列やAPIキー、データベース情報を記載します。**秘匿情報になりうるため、原則として Git 管理対象から除外することを推奨**します（`.gitignore`で除外設定してください）。

- **requirements.txt**  
  プロジェクトで使用する Python パッケージを定義します。 `pip install -r requirements.txt` でインストールします。

- **token.pickle**  
  DatabricksからSnowflakeに接続するためのアクセストークンファイル。NRVANAお知らせページの「Snowflake接続用ファイル」で生成したものを配置してください。**秘匿情報になりうるため、原則として Git 管理対象から除外することを推奨**します（`.gitignore`で除外設定してください）。

## データ準備
（内容作成中）


## 環境変数設定ガイド 🔧

本プロジェクトでは、Databricks から Snowflake にアクセスするために `.env` ファイルを使用します。  
以下の手順に従って `.env` を作成・設定してください。

---

### 1. `.env` ファイルの作成

次の内容をコピーして、プロジェクト直下に `.env` ファイルとして保存してください。

---

### 2. 環境変数項目

#### **Mandatory variables（必須項目）**  
NRVANA 初期設定にて Databricks から Snowflake にアクセスするために生成された `.env` の値を記入してください。

```env
# *** Mandatory variables ***
OAUTH_CLIENT_ID = ''
OAUTH_CLIENT_SECRET = ''
OAUTH_CLIENT_SECRET_2 = ''
OAUTH_REDIRECT_URI = ''
OAUTH_AUTHORIZATION_ENDPOINT = ''
OAUTH_TOKEN_ENDPOINT = ''
API_ENDPOINT = ''
ACCOUNT_IDENTIFER = ''
```

#### **Snowflake VAP DB INFORMATION**  
ご自身の Snowflake 環境の情報を記入してください。（Snowflake コンソールより確認可能）

```env
# **** Snowflake VAP DB INFORMATION ***
SNOWFLAKE_ACCOUNT = ''
SNOWFLAKE_USER = ''
SNOWFLAKE_DB = ''
SNOWFLAKE_SCHEMA_READONLY = ''
SNOWFLAKE_SCHEMA = ''
```

#### **Snowflake VAP TABLE**  
VERITAS で抽出されたテーブル名を記入してください。
地図データはSnowflakeにデータをアップロードした際の設定テーブル名を記入してください。

```env
# **** Snowflake VAP TABLE ***
SNOWFLAKE_TABLE_952W_202410 = 'VERITAS_CAN_OOOOOOOOOOOOOOO'
SNOWFLAKE_TABLE_952W_202504 = 'VERITAS_CAN_OOOOOOOOOOOOOOO'
SNOWFLAKE_TABLE_952W_202510 = 'VERITAS_CAN_OOOOOOOOOOOOOOO'
SNOWFLAKE_TABLE_CIRCUITMODE_AVAILABLE_POI = 'CIRCUIT_MODE_AVAILABLE_POI_202506'
SNOWFLAKE_TABLE_TMI_GRGARAGE_POI = 'JP_GR_GARAGE_LIST'
```

### 3. .env 設定に関する注意点

- `.env` には機密情報が含まれるため、**GitHub へ絶対に push しないでください**。
- `.gitignore` に `.env` が含まれていることを必ず確認してください。
- Databricks 上で動作させる場合は、**Databricks Secrets** に値を登録し、ノートブック側で読み込む方法を推奨します
