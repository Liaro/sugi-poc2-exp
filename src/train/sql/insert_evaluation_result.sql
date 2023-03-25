
CREATE OR REPLACE EXTERNAL TABLE `{{dataset_id}}.evaluation_result_{{exp_name}}_gcs`
(
  rmse FLOAT64 OPTIONS(description='テストデータに対するRMSE'),
  mae FLOAT64 OPTIONS(description='テストデータに対するMAE'),
  r2 FLOAT64 OPTIONS(description='テストデータに対する決定係数'),
  model_version STRING OPTIONS(description='prev_model or current_model'),
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://{{bucket}}/{{evaluation_path}}/evaluation_result_{{exp_name}}.csv'],
  skip_leading_rows = 1
)
;

CREATE TABLE IF NOT EXISTS `{{dataset_id}}.evaluation_result_{{exp_name}}` 
(
  execution_date DATE NOT NULL OPTIONS(description="pipeline実行日(AirFlowのnext_execution_date)"),
  model_version STRING OPTIONS(description='prev_model or current_model'),
  rmse FLOAT64 OPTIONS(description='テストデータに対するRMSE'),
  mae FLOAT64 OPTIONS(description='テストデータに対するMAE'),
  r2 FLOAT64 OPTIONS(description='テストデータに対する決定係数'),
)
PARTITION BY execution_date
OPTIONS(description="testデータに対する現行モデル(prev_model)と最新モデル(current_model)を用いて評価指標の計算結果を格納したテーブル")

;

MERGE
   `{{dataset_id}}.evaluation_result_{{exp_name}}` as target
USING
(
  SELECT
    DATE('{{execution_date}}', "Asia/Tokyo") AS execution_date,
    model_version,
    rmse,
    mae,
    r2,
  FROM `{{dataset_id}}.evaluation_result_{{exp_name}}_gcs`
) AS source
  ON target.execution_date = source.execution_date 
    AND target.model_version = source.model_version   
WHEN MATCHED THEN UPDATE SET
  rmse=source.rmse,
  mae=source.mae,
  r2=source.r2
WHEN NOT MATCHED THEN INSERT ROW
