/*
  前回の処方からの日数に関する特徴
*/
DECLARE END_DATE DATE DEFAULT DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{sum_days - 1}} DAY);
DECLARE START_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL {{train_days + valid_days + test_days + 2 * sum_days}} DAY);

CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.{{script_name}}`
AS

WITH BASE AS (
  SELECT
    yj_code,
    store_code,
    dispensing_date,
    total_dose_by_yj_store,
  FROM `{{project_id}}.{{dataset_id}}.monthly_prescription`
  WHERE
    dispensing_date BETWEEN DATE_SUB(START_DATE, INTERVAL {{max_last_prescription_days}} DAY) AND END_DATE
), INTERVAL_FEATURE AS (
  SELECT
    yj_code, 
    store_code,
    dispensing_date,
    -- testと分布が変わるためcapする
    LEAST
      (DATE_DIFF(dispensing_date, IFNULL(lag_dispensing_date, DATE_SUB(START_DATE, INTERVAL {{max_last_prescription_days}} DAY)), DAY), 
      {{max_last_prescription_days}}
    ) AS day_from_last_prescription,
  FROM (
    SELECT
      *,
      -- 直近で処方が発生した日
      LAST_VALUE(null_dispensing_date IGNORE NULLS) OVER (
        PARTITION BY yj_code, store_code 
        ORDER BY UNIX_DATE(dispensing_date) 
        {% if is_prediction %}
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        {% else %}
        RANGE BETWEEN UNBOUNDED PRECEDING AND {{sum_days}} PRECEDING
        {% endif %}
      ) AS lag_dispensing_date
    FROM (
      SELECT 
        yj_code,
        store_code,
        dispensing_date,
        dispensing_date AS null_dispensing_date
      FROM BASE
      WHERE total_dose_by_yj_store > 0
      UNION ALL
      SELECT 
        yj_code,
        store_code,
        dispensing_date,
        NULL AS null_dispensing_date,
      FROM BASE
      WHERE total_dose_by_yj_store = 0
    )
  )
)
SELECT
  * 
FROM INTERVAL_FEATURE