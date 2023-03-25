
CREATE OR REPLACE EXTERNAL TABLE `{{dataset_id}}.prediction_model_result_{{exp_name}}_gcs`
(
  yj_code	STRING NOT NULL OPTIONS(description="YJコード"),
  store_code STRING NOT NULL OPTIONS(description="店舗コード"),
  dispensing_date DATE NOT NULL OPTIONS(description="処方日"),
  predicted_total_dose FLOAT64 OPTIONS(description="yj_code, store_code単位の(当日含め)未来{{sum_days}}日分の予測処方量"), 
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://{{bucket}}/{{prediction_path}}/predict_result_{{exp_name}}.csv'],
  skip_leading_rows = 1
)
;


CREATE TABLE IF NOT EXISTS `{{dataset_id}}.prediction_model_result_{{exp_name}}` 
(
  yj_code	STRING NOT NULL OPTIONS(description="YJコード"),
  store_code STRING NOT NULL OPTIONS(description="店舗コード"),
  dispensing_date DATE NOT NULL OPTIONS(description="処方日"),
  predicted_total_dose FLOAT64 OPTIONS(description="yj_code, store_code単位の(当日含め)未来{{sum_days}}日分の予測処方量"), 
)
PARTITION BY dispensing_date
OPTIONS(description="{{exp_name}}のモデルによる予測結果を格納したテーブル")

;

MERGE
   `{{dataset_id}}.prediction_model_result_{{exp_name}}` as target
USING
(
  {% if exp_name == 'exp046' %}
  SELECT
    yj_code,
    store_code,
    dispensing_date,
    -- exp046の場合は、スケールを戻す必要がある。
    GREATEST(
      predicted_total_dose 
      * (max_total_dose_by_yj_store - min_total_dose_by_yj_store) 
      + min_total_dose_by_yj_store, 
    0) AS predicted_total_dose,
  FROM `{{dataset_id}}.prediction_model_result_{{exp_name}}_gcs`
  LEFT JOIN `featurestore.min_max_scaler` 
  USING (yj_code, store_code, dispensing_date)
  {% elif exp_name == 'exp047' %}
  SELECT
    yj_code,
    store_code,
    dispensing_date,
    -- exp047の場合は、lagを足す必要がある
    GREATEST(predicted_total_dose + total_dose_by_yj_store, 0) AS predicted_total_dose,
  FROM `{{dataset_id}}.prediction_model_result_{{exp_name}}_gcs`
  LEFT JOIN `prediction_dataset.predict_dataset_exp047`
  USING(yj_code, store_code, dispensing_date)
  {% else %}
  SELECT
    yj_code,
    store_code,
    dispensing_date,
    -- exp042の場合は、LNから戻す必要がある。
    GREATEST(EXP(predicted_total_dose) - 1, 0) AS predicted_total_dose,
  FROM `{{dataset_id}}.prediction_model_result_{{exp_name}}_gcs`
  {% endif %}
) AS source
  ON target.yj_code = source.yj_code 
    AND target.store_code = source.store_code   
    AND target.dispensing_date = source.dispensing_date   
WHEN MATCHED THEN UPDATE SET
  predicted_total_dose=source.predicted_total_dose
WHEN NOT MATCHED THEN INSERT ROW
