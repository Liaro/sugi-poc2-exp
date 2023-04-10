/*
  target関連に関する特徴量を算出 (total_dose, total_price, nunique_patient, age)
*/
DECLARE END_DATE DATE DEFAULT DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{sum_days - 1}} DAY);
DECLARE START_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL {{train_days + valid_days + test_days + 2 * sum_days}} DAY);

CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.{{script_name}}`

AS

WITH BASE AS(
  SELECT
    patient_no,
    yj_code,
    store_code,
    dispensing_date,
    -- 最後に処方があった日付
    LAST_VALUE(lag_dispensing_date IGNORE NULLS) OVER(
      PARTITION BY patient_no, yj_code, store_code ORDER BY dispensing_date
    ) AS lag_dispensing_date,
    -- 最後に処方があった時の処方り
    LAST_VALUE(lag_total_dose IGNORE NULLS) OVER(
      PARTITION BY patient_no, yj_code, store_code ORDER BY dispensing_date
    ) AS lag_total_dose,
    LAST_VALUE(stats_prescription_date IGNORE NULLS) OVER(
      PARTITION BY patient_no, yj_code, store_code ORDER BY dispensing_date
    ) AS filled_stats_prescription_date,
    stats_prescription_date,
    -- 予定処方日
    LAST_VALUE(pred_prescription_date IGNORE NULLS) OVER(
      PARTITION BY patient_no, yj_code, store_code ORDER BY dispensing_date
    ) AS filled_pred_prescription_date,
    pred_prescription_date,
    total_dose,
    total_price,
  FROM (
    SELECT
      patient_no,
      yj_code,
      store_code,
      dispensing_date,
      LAG(if(total_dose > 0, dispensing_date, null)) OVER(PARTITION BY patient_no, yj_code, store_code ORDER BY dispensing_date) AS lag_dispensing_date,
      LAG(if(total_dose > 0, total_dose, null)) OVER(PARTITION BY patient_no, yj_code, store_code ORDER BY dispensing_date) AS lag_total_dose,
      -- 処方のあった次の日からpred_prescription_dateが使えるため
      LAG(if(total_dose > 0, dispensing_date, null)) OVER(PARTITION BY patient_no, yj_code, store_code ORDER BY dispensing_date) AS stats_prescription_date,
      LAG(pred_prescription_date) OVER(PARTITION BY patient_no, yj_code, store_code ORDER BY dispensing_date) AS pred_prescription_date,
      total_dose,
      total_price,
    FROM
      `{{project_id}}.{{dataset_id}}.patient_prescription`
  )
), TEMPLATE as (
  select distinct
    yj_code, dispensing_date,
  from `{{project_id}}.{{dataset_id}}.patient_prescription`
), INTERPOLATE_PRESCRIPTION as (
  -- hi_kaisuが0のものに関して処方間隔の最頻値を求めてそれを用いるためのカラムを作成
  select
    yj_code,
    dispensing_date,
    interval_prescription,
    count(1) as cnt,
  from (
    select
      yj_code,
      dispensing_date,
      array_agg(interval_prescription) over(
        partition by yj_code 
        order by unix_date(dispensing_date)
        range between {{prescription_stats_days}} preceding and 1 preceding
      ) as interval_prescriptions
    from TEMPLATE 
    left join (
      select
        yj_code,
        dispensing_date,
        -- 実際の処方間隔
        date_diff(dispensing_date, lag_dispensing_date, day) as interval_prescription,
      from (
        select
          yj_code,
          store_code,
          patient_no,
          dispensing_date,
          lag(dispensing_date) over(
            partition by yj_code, store_code, patient_no 
            order by dispensing_date
          ) as lag_dispensing_date,
        from BASE
        where total_dose > 0
      )
    ) using(yj_code, dispensing_date)
  ), unnest(interval_prescriptions) as interval_prescription
  where interval_prescription is not null
  group by 1, 2, 3
  -- ある程度カウントがあるものだけを信頼度の高い値とする
  having cnt > 10
  -- 最頻値だけを取得
  qualify row_number() over(partition by yj_code, dispensing_date order by cnt desc) = 1
), ADD_STATS_BASE as (
  -- 最頻値をベースにした次の来店日
  select
    yj_code,
    store_code,
    patient_no,
    dispensing_date,
    lag_dispensing_date,
    lag_total_dose,
    stats_prescription_date,
    pred_prescription_date,
    date_add(filled_stats_prescription_date, interval interval_prescription day) as filled_stats_prescription_date,
    filled_pred_prescription_date,
    total_dose,
  from BASE
  left join INTERPOLATE_PRESCRIPTION using(yj_code, dispensing_date)
), INTERVAL_FEATURE as (
  select
    yj_code,
    store_code,
    patient_no,
    dispensing_date,
    lag_total_dose,
    -- 前回の処方からの日数
    DATE_DIFF(dispensing_date, lag_dispensing_date, DAY) AS day_from_last_dispensing,
    -- 処方が切れると予想された日程との差分 (0に近いほどくる確率が高くなるはず)
    DATE_DIFF(dispensing_date, filled_pred_prescription_date, DAY) AS diff_pred_prescription_date,
    DATE_DIFF(dispensing_date, filled_stats_prescription_date, DAY) AS diff_stats_prescription_date,
    -- その薬品が何回目の処方か
    IFNULL(SUM(IF(total_dose > 0, 1, 0)) OVER (
      PARTITION BY patient_no, yj_code, store_code ORDER BY UNIX_DATE(dispensing_date) 
      RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ), 0) AS num_visit,
  from ADD_STATS_BASE
), PRED_PRESCRIPTION_FEATURE as (
  select
    yj_code,
    store_code,
    pred_prescription_date as dispensing_date,
    count(1) as pred_presciption_count,
    {% for op in ops%}
    {{op}}(lag_total_dose) as pred_presciption_{{op}}_lag_total_dose,
    {% endfor %}
    sum(lag_total_dose) as pred_presciption_sum_lag_total_dose,
  from ADD_STATS_BASE
  where pred_prescription_date is not null
  group by 1, 2, 3
), STATS_PRESCRIPTION_FEATURE as (
  select
    yj_code,
    store_code,
    stats_prescription_date as dispensing_date,
    count(1) as stats_presciption_count,
    {% for op in ops%}
    {{op}}(lag_total_dose) as stats_presciption_{{op}}_lag_total_dose,
    {% endfor %}
    sum(lag_total_dose) as stats_presciption_sum_lag_total_dose,
  from ADD_STATS_BASE
  where stats_prescription_date is not null
  group by 1, 2, 3
)

select
  *
from INTERVAL_FEATURE
left join PRED_PRESCRIPTION_FEATURE using(yj_code, store_code, dispensing_date)
left join STATS_PRESCRIPTION_FEATURE using(yj_code, store_code, dispensing_date)
