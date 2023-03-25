
DECLARE END_DATE DATE DEFAULT DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{sum_days - 1}} DAY);
DECLARE START_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL 1 DAY);


CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.{{script_name}}`
PARTITION BY dispensing_date
CLUSTER BY store_code, yj_code

AS

WITH BASE AS (
  SELECT 
    -- 本番で使用できない特徴を削除
    * except(
      total_price_by_yj_store, 
      nunique_patient_by_yj_store, 
      total_dose_by_yj,
      scaled_total_dose_by_yj,
      total_price_by_yj, 
      nunique_patient_by_yj, 
      total_dose_by_yj_store,
      scaled_total_dose_by_yj_store
    ),
    SIN( 2.0 * acos(-1) * ( dayofyear - 1 ) / 366) AS dayofyear_sin,
    COS( 2.0 * acos(-1) * ( dayofyear - 1 ) / 366) AS dayofyear_cos,
    SIN( 2.0 * acos(-1) * ( day - 1 ) / 31) AS day_sin,
    COS( 2.0 * acos(-1) * ( day - 1 ) / 31) AS day_cos,
    SIN( 2.0 * acos(-1) * ( dayofweek - 1 ) / 7) AS dayofweek_sin,
    COS( 2.0 * acos(-1) * ( dayofweek - 1 ) / 7) AS dayofweek_cos,
    scaled_total_dose_by_yj_store AS total_dose_monthly,
  FROM `{{project_id}}.train_internal.scaled_monthly_prescription` 
  WHERE dispensing_date >= START_DATE AND dispensing_date < END_DATE
)

SELECT
  * except(dispensing_date),
  -- 予測時の日時に変更する
  DATE_ADD(dispensing_date, INTERVAL {{sum_days}} DAY) AS dispensing_date,
FROM BASE 
LEFT JOIN `{{project_id}}.train_internal.predict_scaled_monthly_target_feature` USING(yj_code, store_code, dispensing_date)
LEFT JOIN `{{project_id}}.train_internal.predict_scaled_monthly_category_feature` USING(yj_code, store_code, dispensing_date)
LEFT JOIN `{{project_id}}.train_internal.predict_scaled_monthly_holiday_feature` USING(yj_code, store_code, dispensing_date)
LEFT JOIN `{{project_id}}.train_internal.predict_monthly_last_prescription_feature` USING(yj_code, store_code, dispensing_date)
LEFT JOIN `{{project_id}}.train_internal.doctor_feature` USING(yj_code, store_code)
LEFT JOIN (SELECT jst_date AS dispensing_date, is_holiday, day_type, day_type_sequence FROM`{{project_id}}.import.holiday_master`) USING (dispensing_date)

;

-- 重複チェック
ASSERT (
SELECT
  COUNT(DISTINCT CONCAT(yj_code, store_code, dispensing_date)) = COUNT(*)
FROM `{{project_id}}.{{dataset_id}}.{{script_name}}`
) AS "train date is not unique";

  