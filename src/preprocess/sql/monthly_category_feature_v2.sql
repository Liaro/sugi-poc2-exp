/*
  categoryに関する特徴量を算出 (total_dose)
*/

DECLARE END_DATE DATE DEFAULT DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{sum_days - 1}} DAY);
DECLARE START_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL {{train_days + valid_days + test_days + 2 * sum_days}} DAY);

CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.{{script_name}}`

AS
WITH BASE AS (
  SELECT
    *,
    -- https://www.data-index.co.jp/knowledge/detail1-1.html
    SUBSTR(yj_code, 1, 9) AS yakko_keiro_zaikei,
    SUBSTR(yj_code, 1, 4) AS yakko_bunrui,
    SUBSTR(yj_code, 5, 3) AS touyo_keiro,
    SUBSTR(yj_code, 8, 1) AS zaikei,
  FROM 
    `{{project_id}}.{{dataset_id}}.monthly_prescription`
  WHERE
    -- 統計量算出に使用するデータまで読み込む
    dispensing_date BETWEEN DATE_SUB(START_DATE, INTERVAL {{sum_days + (preceding_days | max)}} DAY) AND END_DATE

)

SELECT
  *
FROM (
  -- 過去期間における統計量
  SELECT
    yj_code,
    store_code,
    dispensing_date,
    {% for op in category_ops %}
      {% for day in preceding_days %}
        -- yj_code, store, dayofweek単位の特徴 (あんま曜日の時系列性なさそう？)
        {{op}}(total_dose_by_yj_store) OVER (
            PARTITION BY yj_code, store_code, dayofweek
            {% if is_prediction %}
            -- 予測時には、予測したい日付の{{sum_days}}日前までしかmonthly_prescriptionにデータが挿入されていない。
            -- そのため、訓練時と特徴量を一致させるために、WINDOW区間を調整する。
            ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day}} PRECEDING AND CURRENT ROW
            {% else %}
            ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day + sum_days}} PRECEDING AND {{sum_days}} PRECEDING
            {% endif %}
        ) AS total_dose_by_yj_store_dow_{{op}}_{{day}},
        -- yj_code, dayofweek単位の特徴 (あんま曜日の時系列性なさそう？)
        {{op}}(total_dose_by_yj) OVER (
            PARTITION BY yj_code, dayofweek
            {% if is_prediction %}
            -- 予測時には、予測したい日付の{{sum_days}}日前までしかmonthly_prescriptionにデータが挿入されていない。
            -- そのため、訓練時と特徴量を一致させるために、WINDOW区間を調整する。
            ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day}} PRECEDING AND CURRENT ROW
            {% else %}
            ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day + sum_days}} PRECEDING AND {{sum_days}} PRECEDING
            {% endif %}
        ) AS total_dose_by_yj_dow_{{op}}_{{day}},
        -- general_name, store_code単位の特徴
        {{op}}(total_dose_by_yj_store) OVER (
            PARTITION BY general_name, store_code
            {% if is_prediction %}
            -- 予測時には、予測したい日付の{{sum_days}}日前までしかmonthly_prescriptionにデータが挿入されていない。
            -- そのため、訓練時と特徴量を一致させるために、WINDOW区間を調整する。
            ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day}} PRECEDING AND CURRENT ROW
            {% else %}
            ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day + sum_days}} PRECEDING AND {{sum_days}} PRECEDING
            {% endif %}
        ) AS total_dose_by_name_store_{{op}}_{{day}},
      {% endfor %}
    {% endfor %}
  FROM
)
WHERE
  dispensing_date >= START_DATE AND dispensing_date < END_DATE
