
-- end_tsに最新の日付が入ってくるため、end_tsの日付の値が挿入されるように1日ずらす
DECLARE START_DATE DATE DEFAULT DATE_ADD(DATE("{{start_ts}}", "Asia/Tokyo"), INTERVAL 1 DAY);
DECLARE END_DATE DATE DEFAULT DATE_ADD(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL 1 DAY);


CREATE TABLE IF NOT EXISTS `predicted.{{script_name}}` 
(
  yj_code	STRING NOT NULL OPTIONS(description="YJコード"),
  store_code STRING NOT NULL OPTIONS(description="店舗コード"),
  pred_exe_date DATE NOT NULL OPTIONS(description="予測実行日"),
  pred_period STRING NOT NULL OPTIONS(description="予測日数"),
  pred_period_start DATE NOT NULL OPTIONS(description="予測期間開始日"),
  pred_period_end DATE NOT NULL OPTIONS(description="予測期間終了日"),
  model_id STRING NOT NULL OPTIONS(description="予測に使用されている実験id"),
  predicted_values FLOAT64 OPTIONS(description="yj_code, store_code単位の(当日含め)未来{{sum_days}}日分の予測処方量"),
)
PARTITION BY pred_exe_date
CLUSTER BY store_code, yj_code
OPTIONS(description="複数のモデルの予測結果を統合したテーブル")

;

MERGE
   `predicted.{{script_name}}` as target
USING
(
  SELECT
    yj_code,
    store_code,
    dispensing_date AS pred_exe_date,
    CAST({{sum_days}} AS STRING) AS pred_period,
    dispensing_date AS pred_period_start,
    DATE_ADD(dispensing_date, INTERVAL {{sum_days}} DAY) AS pred_period_end,
    CASE 
      WHEN model_abc_flag = 'a' THEN 'exp047'
      ELSE 'exp042'
    END AS model_id,
    CASE 
      WHEN model_abc_flag = 'a' THEN exp047.predicted_total_dose
      ELSE exp042.predicted_total_dose
    END AS predicted_values,
  FROM `predicted.prediction_model_result_exp047` AS exp047
  LEFT JOIN `predicted.prediction_model_result_exp042` AS exp042
  USING (yj_code, store_code, dispensing_date)
  LEFT JOIN (SELECT DATE_ADD(dispensing_date, INTERVAL 1 DAY) as dispensing_date, yj_code, store_code, model_abc_flag FROM `prediction_internal.date_store_yj_abc` )
  USING (yj_code, store_code, dispensing_date)
  WHERE
    dispensing_date >= START_DATE AND dispensing_date < END_DATE
) AS source
  ON target.yj_code = source.yj_code 
    AND target.store_code = source.store_code   
    AND target.pred_exe_date = source.pred_exe_date   
WHEN MATCHED THEN UPDATE SET
  model_id=source.model_id,
  predicted_values=source.predicted_values
WHEN NOT MATCHED THEN INSERT ROW
