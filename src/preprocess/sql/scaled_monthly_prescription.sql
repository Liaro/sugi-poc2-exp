/*
  importテーブルから必要なテーブルをまとめて、処方量をyj_code, store_codeごとにまとめたもの。
  処方量は{{sum_days}}日分だけ未来の値まで積算されている。
*/

CREATE OR REPLACE VIEW `{{project_id}}.{{dataset_id}}.{{script_name}}` 

AS

-- TODO: min_max_scalerに予測日時がなかったらASSERTで落とす

WITH MIN_MAX AS (
  {% if is_prediction %}
  -- 予測時は、訓練時に用いた値を使う
  SELECT
    yj_code,
    store_code,
    dispensing_date,
    min_total_dose_by_yj_store,
    max_total_dose_by_yj_store,
    min_total_dose_by_yj,
    max_total_dose_by_yj,
  FROM `{{project_id}}.featurestore.min_max_scaler`
  -- view内ではスクリプト変数を扱えない
  WHERE dispensing_date >= DATE("{{start_ts}}", "Asia/Tokyo") AND dispensing_date < DATE("{{end_ts}}", "Asia/Tokyo")
  {% else %}
  SELECT
    yj_code,
    store_code,
    MIN(total_dose_by_yj_store) AS min_total_dose_by_yj_store,
    MAX(total_dose_by_yj_store) AS max_total_dose_by_yj_store,
    MIN(total_dose_by_yj) AS min_total_dose_by_yj,
    MAX(total_dose_by_yj) AS max_total_dose_by_yj,
  FROM `{{project_id}}.{{dataset_id}}.monthly_prescription`
  -- view内ではスクリプト変数を扱えない
  WHERE DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{train_days + valid_days + test_days + 2 * sum_days}} DAY) <= dispensing_date 
    AND dispensing_date < DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{valid_days + test_days + 2 * sum_days}} DAY)
  GROUP BY yj_code, store_code
  {% endif %}
) 
SELECT
  raw.*,
  CASE 
    WHEN max_total_dose_by_yj_store = min_total_dose_by_yj_store THEN 0
    ELSE (total_dose_by_yj_store - min_total_dose_by_yj_store) / (max_total_dose_by_yj_store - min_total_dose_by_yj_store) 
  END AS scaled_total_dose_by_yj_store,
  CASE WHEN max_total_dose_by_yj = min_total_dose_by_yj THEN 0
  ELSE (total_dose_by_yj - min_total_dose_by_yj) / (max_total_dose_by_yj - min_total_dose_by_yj) 
  END AS scaled_total_dose_by_yj,
FROM `{{project_id}}.{{dataset_id}}.monthly_prescription` as raw
{% if is_prediction %}
LEFT JOIN MIN_MAX USING (yj_code, store_code, dispensing_date)
{% else %}
LEFT JOIN MIN_MAX USING (yj_code, store_code)
{% endif %}
