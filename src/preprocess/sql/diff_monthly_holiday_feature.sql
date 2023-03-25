/*
  祝日に関する特徴量を算出 (total_dose)
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
        -- yj_code, store, day_type_sequence単位の特徴 (あんま曜日の時系列性なさそう？)
        {{op}}(diff_total_dose_by_yj_store) OVER (
            PARTITION BY yj_code, store_code, day_type_sequence
            {% if is_prediction %}
            -- 予測時には、予測したい日付の{{sum_days}}日前までしかmonthly_prescriptionにデータが挿入されていない。
            -- そのため、訓練時と特徴量を一致させるために、WINDOW区間を調整する。
            ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day}} PRECEDING AND CURRENT ROW
            {% else %}
            ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day + sum_days}} PRECEDING AND {{sum_days}} PRECEDING
            {% endif %}
        ) AS total_dose_by_yj_store_dts_{{op}}_{{day}},
        -- yj_code, day_type_sequence単位の特徴 (あんま曜日の時系列性なさそう？)
        {{op}}(diff_total_dose_by_yj) OVER (
            PARTITION BY yj_code, day_type_sequence
            {% if is_prediction %}
            -- 予測時には、予測したい日付の{{sum_days}}日前までしかmonthly_prescriptionにデータが挿入されていない。
            -- そのため、訓練時と特徴量を一致させるために、WINDOW区間を調整する。
            ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day}} PRECEDING AND CURRENT ROW
            {% else %}
            ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day + sum_days}} PRECEDING AND {{sum_days}} PRECEDING
            {% endif %}
        ) AS total_dose_by_yj_dts_{{op}}_{{day}},
        -- general_name, store_code, day_type_sequence単位の特徴
        {{op}}(diff_total_dose_by_yj_store) OVER (
            PARTITION BY general_name, store_code, day_type_sequence
            {% if is_prediction %}
            -- 予測時には、予測したい日付の{{sum_days}}日前までしかmonthly_prescriptionにデータが挿入されていない。
            -- そのため、訓練時と特徴量を一致させるために、WINDOW区間を調整する。
            ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day}} PRECEDING AND CURRENT ROW
            {% else %}
            ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN {{day + sum_days}} PRECEDING AND {{sum_days}} PRECEDING
            {% endif %}
        ) AS total_dose_by_name_store_dts_{{op}}_{{day}},
      {% endfor %}
    {% endfor %}
  FROM
    `{{project_id}}.{{dataset_id}}.diff_monthly_prescription` AS base
  LEFT JOIN `{{project_id}}.import.holiday_master` AS holiday ON base.dispensing_date = holiday.jst_date
  WHERE
    -- 統計量算出に使用するデータまで読み込む
    dispensing_date BETWEEN DATE_SUB(START_DATE, INTERVAL {{sum_days + (preceding_days | max)}} DAY) AND END_DATE
)
WHERE
  dispensing_date >= START_DATE AND dispensing_date < END_DATE
