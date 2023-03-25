DECLARE TRAIN_START_DATE DATE DEFAULT DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{train_days + valid_days + test_days + 2 * sum_days}} DAY);
DECLARE TRAIN_END_DATE DATE DEFAULT DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{valid_days + test_days + 2 * sum_days}} DAY);
-- 予測で使われる日付 (訓練を行った日付から)
DECLARE START_DATE DATE DEFAULT DATE("{{end_ts}}", "Asia/Tokyo");
DECLARE END_DATE DATE DEFAULT DATE_ADD(START_DATE, INTERVAL {{update_days}} DAY);


CREATE TABLE IF NOT EXISTS `{{project_id}}.{{dataset_id}}.{{script_name}}`(
  yj_code	STRING NOT NULL OPTIONS(description="YJコード"),
  store_code STRING NOT NULL OPTIONS(description="店舗コード"),
  dispensing_date DATE NOT NULL OPTIONS(description="予測に使われる日付"),
  min_total_dose_by_yj_store FLOAT64 NOT NULL OPTIONS(description="yj_code, store_code単位の処方量の訓練期間における最小値"),
  max_total_dose_by_yj_store FLOAT64 NOT NULL OPTIONS(description="yj_code, store_code単位の処方量の訓練期間における最大値"),
  min_total_dose_by_yj FLOAT64 NOT NULL OPTIONS(description="yj_code単位の処方量の訓練期間における最小値"),
  max_total_dose_by_yj FLOAT64 NOT NULL OPTIONS(description="yj_code単位の処方量の訓練期間における最大値")
)
PARTITION BY dispensing_date
CLUSTER BY store_code
OPTIONS(description="訓練期間における処方量の最小値最大値。スケーリングに用いる。")
;

DELETE {{dataset_id}}.{{script_name}}
WHERE START_DATE <= dispensing_date AND dispensing_date < END_DATE
;

INSERT {{dataset_id}}.{{script_name}}


WITH MIN_MAX AS (
  SELECT
    yj_code,
    store_code,
    MIN(total_dose_by_yj_store) AS min_total_dose_by_yj_store,
    MAX(total_dose_by_yj_store) AS max_total_dose_by_yj_store,
    MIN(total_dose_by_yj) AS min_total_dose_by_yj,
    MAX(total_dose_by_yj) AS max_total_dose_by_yj,
  FROM `{{project_id}}.train_internal.monthly_prescription`
  WHERE TRAIN_START_DATE<= dispensing_date AND dispensing_date < TRAIN_END_DATE
  GROUP BY yj_code, store_code
) 
SELECT
  yj_code,
  store_code,
  dispensing_date,
  min_total_dose_by_yj_store,
  max_total_dose_by_yj_store,
  min_total_dose_by_yj,
  min_total_dose_by_yj,
FROM MIN_MAX,
UNNEST(GENERATE_DATE_ARRAY(START_DATE, END_DATE, INTERVAL 1 DAY)) AS dispensing_date
