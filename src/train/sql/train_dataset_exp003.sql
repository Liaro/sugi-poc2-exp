
DECLARE END_DATE DATE DEFAULT DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{sum_days}} DAY);
DECLARE TRAIN_START_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL {{train_days + valid_days + test_days + 2 * sum_days}} DAY);
DECLARE TRAIN_END_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL {{valid_days + test_days + 2 * sum_days}} DAY);
DECLARE VALID_START_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL {{valid_days + test_days + sum_days}} DAY);
DECLARE VALID_END_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL {{test_days + sum_days}} DAY);
DECLARE TEST_START_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL {{test_days}} DAY);


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
      total_dose_by_yj_store,
      total_price_by_yj, 
      nunique_patient_by_yj
    ),
    SIN( 2.0 * acos(-1) * ( dayofyear - 1 ) / 366) AS dayofyear_sin,
    COS( 2.0 * acos(-1) * ( dayofyear - 1 ) / 366) AS dayofyear_cos,
    SIN( 2.0 * acos(-1) * ( day - 1 ) / 31) AS day_sin,
    COS( 2.0 * acos(-1) * ( day - 1 ) / 31) AS day_cos,
    SIN( 2.0 * acos(-1) * ( dayofweek - 1 ) / 7) AS dayofweek_sin,
    COS( 2.0 * acos(-1) * ( dayofweek - 1 ) / 7) AS dayofweek_cos,
    -- log化
    LN(total_dose_by_yj_store + 1) AS total_dose_monthly,
    CASE 
      WHEN dispensing_date >= TRAIN_START_DATE AND dispensing_date < TRAIN_END_DATE THEN 'train'
      WHEN dispensing_date >= VALID_START_DATE AND dispensing_date < VALID_END_DATE THEN 'valid'
      WHEN dispensing_date >= TEST_START_DATE THEN 'test'
      -- train, validの間、valid, testの間がここに落ちる
      ELSE NULL
    END AS split_flag,
  FROM `{{project_id}}.{{dataset_id}}.monthly_prescription` 
  WHERE dispensing_date >= TRAIN_START_DATE AND dispensing_date <= END_DATE
), TRAIN_DATA AS (
  -- 処方が無いところの影響を減らすためにサンプリングを行う
  SELECT * FROM BASE 
  WHERE total_dose_monthly = 0 AND split_flag != 'test'
  -- 1/{{sample_rate}} にサンプリング
  QUALIFY MOD(ROW_NUMBER() OVER(PARTITION BY yj_code, store_code), {{sample_rate}}) = 0
  UNION ALL
  SELECT * FROM BASE 
  WHERE total_dose_monthly > 0 AND split_flag != 'test'
  UNION ALL
  SELECT * FROM BASE 
  WHERE split_flag = 'test'
)

SELECT
  *,
FROM TRAIN_DATA 
LEFT JOIN `{{project_id}}.{{dataset_id}}.monthly_target_feature` USING(yj_code, store_code, dispensing_date)
LEFT JOIN `{{project_id}}.{{dataset_id}}.monthly_category_feature` USING(yj_code, store_code, dispensing_date)
LEFT JOIN `{{project_id}}.{{dataset_id}}.monthly_holiday_feature` USING(yj_code, store_code, dispensing_date)
LEFT JOIN `{{project_id}}.{{dataset_id}}.monthly_last_prescription_feature` USING(yj_code, store_code, dispensing_date)
LEFT JOIN `{{project_id}}.{{dataset_id}}.patient_agg_feature` USING(yj_code, store_code, dispensing_date)
LEFT JOIN `{{project_id}}.{{dataset_id}}.doctor_feature` USING(yj_code, store_code)
LEFT JOIN (SELECT jst_date AS dispensing_date, is_holiday, day_type, day_type_sequence FROM`{{project_id}}.import.holiday_master`) USING (dispensing_date)

;

-- 重複チェック
ASSERT (
SELECT
  COUNT(DISTINCT CONCAT(yj_code, store_code, dispensing_date)) = COUNT(*)
FROM `{{project_id}}.{{dataset_id}}.{{script_name}}`
) AS "train date is not unique";

  