
-- window系の特徴も追加
CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.{{script_name}}`

AS

with base as (
  select
    yj_code,
    store_code,
    patient_no,
    dispensing_date,
    day_from_last_dispensing,
    diff_pred_prescription_date,
    diff_stats_prescription_date,
    num_visit,
    lag_total_dose,
    pred_presciption_count,
    stats_presciption_count,
    {% for op in ops%}
    pred_presciption_{{op}}_lag_total_dose,
    stats_presciption_{{op}}_lag_total_dose,
    {% endfor %}
    pred_presciption_sum_lag_total_dose,
    stats_presciption_sum_lag_total_dose,
  from `{{project_id}}.{{dataset_id}}.patient_feature`
  where num_visit > 0
), agg as (
  select
    yj_code, 
    store_code,
    dispensing_date,
    {% for op in ops %}
      {{op}}(diff_pred_prescription_date) as {{op}}_diff_pred_prescription_date_yj_store,
      {{op}}(diff_stats_prescription_date) as {{op}}_diff_stats_prescription_date_yj_store,
      {{op}}(num_visit) as {{op}}_num_visit_yj_store,
      {{op}}(day_from_last_dispensing) as {{op}}_day_from_last_dispensing_yj_store,
    {% endfor %}
  from base
  group by 1, 2, 3
), window_agg as (
  select distinct
    yj_code, 
    store_code,
    dispensing_date,
    {% for op in ops %}
      {% for stats_day in stats_days %}
        {{op}}(diff_pred_prescription_date) over (
          partition by yj_code, store_code
          order by unix_date(dispensing_date)
          range between {{stats_day + 7}} preceding and 7 preceding
        ) as {{op}}_diff_pred_prescription_date_{{stats_day}}days_yj_store,
        {{op}}(diff_stats_prescription_date) over (
          partition by yj_code, store_code
          order by unix_date(dispensing_date)
          range between {{stats_day + 7}} preceding and 7 preceding
        ) as {{op}}_diff_stats_prescription_date_{{stats_day}}days_yj_store,
        {{op}}(num_visit) over (
          partition by yj_code, store_code
          order by unix_date(dispensing_date)
          range between {{stats_day + 7}} preceding and 7 preceding
        ) as {{op}}_num_visit_{{stats_day}}days_yj_store,
        {{op}}(day_from_last_dispensing) over (
          partition by yj_code, store_code
          order by unix_date(dispensing_date)
          range between {{stats_day + 7}} preceding and 7 preceding
        ) as {{op}}_day_from_last_dispensing_{{stats_day}}days_yj_store,
      {% endfor %}
    {% endfor %}
  from base
), dose_agg as (
  select
    *,
    {% for op in ops %}
      {% for stats_day in stats_days %}
        {{op}}(pred_presciption_sum_lag_total_dose) over (
          partition by yj_code, store_code
          order by unix_date(dispensing_date)
          range between {{stats_day + 7}} preceding and 7 preceding
        ) as {{op}}_pred_presciption_sum_lag_total_dose_{{stats_day}}days_yj_store,
        {{op}}(stats_presciption_sum_lag_total_dose) over (
          partition by yj_code, store_code
          order by unix_date(dispensing_date)
          range between {{stats_day + 7}} preceding and 7 preceding
        ) as {{op}}_stats_presciption_sum_lag_total_dose_{{stats_day}}days_yj_store,
      {% endfor %}
    {% endfor %}
  from (
    select distinct
      yj_code, 
      store_code,
      dispensing_date,
      {% for op in ops%}
      pred_presciption_{{op}}_lag_total_dose,
      stats_presciption_{{op}}_lag_total_dose,
      {% endfor %}
      pred_presciption_sum_lag_total_dose,
      stats_presciption_sum_lag_total_dose,
    from base
  )
) 

select
  *
from agg
left join window_agg using(yj_code, store_code, dispensing_date)
left join dose_agg using(yj_code, store_code, dispensing_date)

;

assert (
  select 
    count(distinct concat(yj_code, store_code, dispensing_date)) = count(1)
  from `{{project_id}}.{{dataset_id}}.{{script_name}}`
)