/*
  target関連に関する特徴量を算出 (total_dose, total_price, nunique_patient, age)
*/
{% if is_prediction %}
DECLARE END_DATE DATE DEFAULT DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{sum_days - 1}} DAY);
DECLARE START_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL 1 DAY);
CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.predict_{{script_name}}`

{% else %}
DECLARE END_DATE DATE DEFAULT DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{sum_days - 1}} DAY);
DECLARE START_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL {{train_days + valid_days + test_days + 2 * sum_days}} DAY);

CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.{{script_name}}`
{% endif %}

AS

WITH BASE AS(
  SELECT
    *
  FROM
    `{{project_id}}.{{dataset_id}}.monthly_prescription`
  WHERE
    -- 統計量算出に使用するデータまで読み込む
    -- 基本的にlagに必要なデータ < 統計値算出に必要なデータなのでこれで問題ない。
    dispensing_date BETWEEN DATE_SUB(START_DATE, INTERVAL {{sum_days + (preceding_days | max)}} DAY) AND END_DATE
), STATS_FEATURE AS (
  -- 過去期間における統計量
  SELECT
    yj_code,
    store_code,
    dispensing_date,
    {% for op in ops %}
      {% for day in preceding_days %}
        -- yj_code, store単位の特徴
        {{op}}(total_dose_by_yj_store) OVER (yj_store_window_{{day}}) AS total_dose_by_yj_store_{{op}}_{{day}},
        {{op}}(total_price_by_yj_store) OVER (yj_store_window_{{day}}) AS total_price_by_yj_store_{{op}}_{{day}},
        {{op}}(nunique_patient_by_yj_store) OVER (yj_store_window_{{day}}) AS nunique_patient_by_yj_store_{{op}}_{{day}},
        -- yj_code単位の特徴
        {{op}}(total_dose_by_yj) OVER (yj_window_{{day}}) AS total_dose_by_yj_{{op}}_{{day}},
        {{op}}(total_price_by_yj) OVER (yj_window_{{day}}) AS total_price_by_yj_{{op}}_{{day}},
        {{op}}(nunique_patient_by_yj) OVER (yj_window_{{day}}) AS nunique_patient_by_yj_{{op}}_{{day}},
      {% endfor %}
    {% endfor %}
    FROM 
      BASE
    WINDOW
      {% for day in preceding_days %}
        {% if is_prediction %}
        -- 予測時には、予測したい日付の{{sum_days}}日前までしかmonthly_prescriptionにデータが挿入されていない。
        -- そのため、訓練時と特徴量を一致させるために、WINDOW区間を調整する。
        yj_store_window_{{day}} AS (
          PARTITION BY yj_code, store_code 
          ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day}} PRECEDING AND CURRENT ROW
        ),
        yj_window_{{day}} AS (
          PARTITION BY yj_code 
          ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day}} PRECEDING AND CURRENT ROW
        ){% if not loop.last %}, {% endif %}
        {% else %}
        yj_store_window_{{day}} AS (
          PARTITION BY yj_code, store_code 
          ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day + sum_days}} PRECEDING AND {{sum_days}} PRECEDING
        ),
        yj_window_{{day}} AS (
          PARTITION BY yj_code, store_code 
          ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day + sum_days}} PRECEDING AND {{sum_days}} PRECEDING
        ){% if not loop.last %}, {% endif %}
        {% endif %}
      {% endfor %}
), LAG_FEATURE AS (
  SELECT
    yj_code,
    store_code,
    dispensing_date,
    {% for day in lag_days %}
      -- yj_code, store単位の特徴
      LAST_VALUE(total_dose_by_yj_store) OVER (yj_store_window_{{day}}) AS lag{{day}}_total_dose_by_yj_store,
      LAST_VALUE(total_price_by_yj_store) OVER (yj_store_window_{{day}}) AS lag{{day}}_total_price_by_yj_store,
      LAST_VALUE(nunique_patient_by_yj_store) OVER (yj_store_window_{{day}}) AS lag{{day}}_nunique_patient_by_yj_store,
      -- yj_code単位の特徴
      LAST_VALUE(total_dose_by_yj) OVER (yj_store_window_{{day}}) AS lag{{day}}_total_dose_by_yj,
      LAST_VALUE(total_price_by_yj) OVER (yj_store_window_{{day}}) AS lag{{day}}_total_price_by_yj,
      LAST_VALUE(nunique_patient_by_yj) OVER (yj_store_window_{{day}}) AS lag{{day}}_nunique_patient_by_yj,
    {% endfor %}
  FROM 
    BASE
  WINDOW
    {% for day in lag_days %}
      {% if is_prediction %}
      -- 予測時には、予測したい日付の{{sum_days}}日前までしかmonthly_prescriptionにデータが挿入されていない。
      -- そのため、訓練時と特徴量を一致させるために、WINDOW区間を調整する。
      yj_store_window_{{day}} AS (
        PARTITION BY yj_code, store_code 
        ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN UNBOUNDED PRECEDING AND {{day}}  PRECEDING
      ){% if not loop.last %}, {% endif %}
      {% else %}
      yj_store_window_{{day}} AS (
        PARTITION BY yj_code, store_code 
        ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN UNBOUNDED PRECEDING AND {{day + sum_days}}  PRECEDING
      ){% if not loop.last %}, {% endif %}
      {% endif %}
    {% endfor %}
), NON_ZERO_LAG_FEATURE AS (
  SELECT
    yj_code, 
    store_code, 
    dispensing_date,
    -- backfill
    LAST_VALUE(lag_non_zero_total_dose_by_yj_store IGNORE NULLS) OVER (
        PARTITION BY yj_code, store_code 
        ORDER BY UNIX_DATE(dispensing_date)
        {% if is_prediction %}
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        {% else %}
        RANGE BETWEEN UNBOUNDED PRECEDING AND {{sum_days}} PRECEDING
        {% endif %}
    ) AS lag_non_zero_total_dose_by_yj_store,
    LAST_VALUE(lag_non_zero_total_dose_by_yj IGNORE NULLS) OVER (
        PARTITION BY yj_code 
        ORDER BY UNIX_DATE(dispensing_date)
        {% if is_prediction %}
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        {% else %}
        RANGE BETWEEN UNBOUNDED PRECEDING AND {{sum_days}} PRECEDING
        {% endif %}
    ) AS lag_non_zero_total_dose_by_yj,
  FROM `{{project_id}}.{{dataset_id}}.monthly_prescription`
  LEFT JOIN (
    SELECT
      yj_code,
      store_code,
      dispensing_date,
      -- yj_code, store_code単位の特徴 (ゼロじゃない値)
      LAST_VALUE(total_dose_by_yj_store) OVER (
          PARTITION BY yj_code, store_code  
          ORDER BY UNIX_DATE(dispensing_date)
          {% if is_prediction %}
          RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          {% else %}
          RANGE BETWEEN UNBOUNDED PRECEDING AND {{sum_days}} PRECEDING
          {% endif %}
      ) AS lag_non_zero_total_dose_by_yj_store,
      -- yj_code単位の特徴 (ゼロじゃない値)
      LAST_VALUE(total_dose_by_yj) OVER (
          PARTITION BY yj_code  
          ORDER BY UNIX_DATE(dispensing_date)
          {% if is_prediction %}
          RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          {% else %}
          RANGE BETWEEN UNBOUNDED PRECEDING AND {{sum_days}} PRECEDING
          {% endif %}
      ) AS lag_non_zero_total_dose_by_yj,
    FROM 
      `{{project_id}}.{{dataset_id}}.monthly_prescription`
    WHERE 
      total_dose_by_yj_store != 0
  )
  USING (yj_code, store_code, dispensing_date)
  WHERE
    -- LAG算出に使用するデータまで読み込む
    dispensing_date BETWEEN DATE_SUB(START_DATE, INTERVAL {{sum_days + (preceding_days | max)}} DAY) AND END_DATE
), DIFF_FEATURE AS (
  SELECT
    yj_code, 
    store_code, 
    dispensing_date,
    {% for preceding_day in preceding_days %}
      {% for lag_day in lag_days %}
        lag{{lag_day}}_total_dose_by_yj_store 
        - total_dose_by_yj_store_avg_{{preceding_day}} AS lag{{lag_day}}_minus_avg{{preceding_day}}_by_yj_store,
        lag{{lag_day}}_total_dose_by_yj 
        - total_dose_by_yj_avg_{{preceding_day}} AS lag{{lag_day}}_minus_avg{{preceding_day}}_by_yj,
      {% endfor %}
      lag_non_zero_total_dose_by_yj_store 
      - total_dose_by_yj_store_avg_{{preceding_day}} AS lag_non_zero_minus_avg{{preceding_day}}_by_yj_store,
    {% endfor %}
  FROM STATS_FEATURE
  LEFT JOIN LAG_FEATURE USING (yj_code, store_code, dispensing_date)
  LEFT JOIN NON_ZERO_LAG_FEATURE USING (yj_code, store_code, dispensing_date)
)
-- main
SELECT
  *
FROM (
  SELECT
    stats_ft.*,
    lag_ft.* except(yj_code, store_code, dispensing_date),
    non_zero_lag_ft.* except(yj_code, store_code, dispensing_date),
    diff_ft.* except(yj_code, store_code, dispensing_date),
  FROM STATS_FEATURE AS stats_ft
  LEFT JOIN LAG_FEATURE  AS lag_ft
  USING (yj_code, store_code, dispensing_date)
  LEFT JOIN NON_ZERO_LAG_FEATURE  AS non_zero_lag_ft
  USING (yj_code, store_code, dispensing_date)
  LEFT JOIN DIFF_FEATURE  AS diff_ft
  USING (yj_code, store_code, dispensing_date)
)
WHERE
  dispensing_date >= START_DATE AND dispensing_date < END_DATE
