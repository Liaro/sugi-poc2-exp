DECLARE END_DATE DATE DEFAULT DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{sum_days}} DAY);
DECLARE START_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL {{test_days}} DAY);

CREATE TABLE IF NOT EXISTS `{{project_id}}.{{dataset_id}}.{{script_name}}`(
  yj_code	STRING NOT NULL OPTIONS(description="YJコード"),
  store_code STRING NOT NULL OPTIONS(description="店舗コード"),
  dispensing_date DATE NOT NULL OPTIONS(description="予測に使われる日付"),
  sum_total_dose FLOAT64 NOT NULL OPTIONS(description="yj_code, store_code単位の過去{{abc_stats_days}}日分の処方量合計"),
  count_prescription INT64 NOT NULL OPTIONS(description="yj_code, store_code単位の過去{{abc_stats_days}}日分の処方数合計"),
  total_dose_rank_by_yj FLOAT64 NOT NULL OPTIONS(description="sum_total_dose基準でstore_code関係なしにdispensing_date単位のパーセントランク"),
  total_dose_rank_by_yj_store FLOAT64 NOT NULL OPTIONS(description="sum_total_dose基準でstore_code, dispensing_date単位のパーセントランク"),
  count_rank_by_yj FLOAT64 NOT NULL OPTIONS(description="count_prescription基準でstore_code, dispensing_date単位のパーセントランク"),
  count_rank_by_yj_store FLOAT64 NOT NULL OPTIONS(description="count_prescription基準でstore_code, dispensing_date単位のパーセントランク"),
  model_abc_flag STRING NOT NULL OPTIONS(description="モデルを分割する際に用いるABCフラグ (total_dose_rank_by_yj >= 0.3 -> a, 0.1 <= total_dose_rank_by_yj < 0.3 -> b, total_dose_rank_by_yj < 0.1 -> c)"),
  data_abc_flag STRING NOT NULL OPTIONS(description="分析の際に用いるABCフラグ (count_rank_by_yj_store >= 0.3 -> a, 0.1 <= count_rank_by_yj_store < 0.3 -> b, count_rank_by_yj_store < 0.1 -> c)"),
)
PARTITION BY dispensing_date
CLUSTER BY store_code
OPTIONS(description="分析、アンサンブルに用いるABCフラグ")
;

DELETE  `{{project_id}}.{{dataset_id}}.{{script_name}}`
WHERE START_DATE <= dispensing_date AND dispensing_date < END_DATE
;

INSERT  `{{project_id}}.{{dataset_id}}.{{script_name}}`


WITH PRES_COUNT AS (
  SELECT DISTINCT
    yj_code,
    store_code,
    dispensing_date,
    SUM(total_dose_by_yj_store) OVER(
      PARTITION BY yj_code, store_code 
      ORDER BY UNIX_DATE(dispensing_date)
      RANGE BETWEEN {{abc_stats_days}} PRECEDING AND 1 PRECEDING
    ) AS sum_total_dose,
    SUM(IFNULL(nunique_patient_by_yj_store, 0)) OVER(
      PARTITION BY yj_code, store_code 
      ORDER BY UNIX_DATE(dispensing_date)
      RANGE BETWEEN {{abc_stats_days}} PRECEDING AND 1 PRECEDING
    ) AS count_prescription,
  FROM `{{project_id}}.train_internal.monthly_prescription`
  WHERE 
    dispensing_date >= DATE_SUB(START_DATE, INTERVAL {{abc_stats_days}} DAY)
    AND dispensing_date < DATE_SUB(END_DATE, INTERVAL {{sum_days - 1}} DAY)
), DOSE_RANK AS (
  SELECT
    yj_code,
    store_code,
    dispensing_date,
    IF(sum_total_dose_sum_by_yj!=0, sum_total_dose_cumsum_by_yj / sum_total_dose_sum_by_yj, 0) AS total_dose_rank_by_yj,
    IF(count_prescription_sum_by_yj!=0, count_prescription_cumsum_by_yj / count_prescription_sum_by_yj, 0) AS count_rank_by_yj,
    IF(sum_total_dose_sum_by_yj_store!=0, sum_total_dose_cumsum_by_yj_store / sum_total_dose_sum_by_yj_store, 0) AS total_dose_rank_by_yj_store,
    IF(count_prescription_sum_by_yj_store!=0, count_prescription_cumsum_by_yj_store / count_prescription_sum_by_yj_store, 0) AS count_rank_by_yj_store,
  FROM (
    SELECT DISTINCT
      yj_code,
      store_code,
      dispensing_date,
      SUM(sum_total_dose) OVER(PARTITION BY dispensing_date ORDER BY sum_total_dose) AS sum_total_dose_cumsum_by_yj,
      SUM(sum_total_dose) OVER(PARTITION BY dispensing_date) AS sum_total_dose_sum_by_yj,
      SUM(count_prescription) OVER(PARTITION BY dispensing_date ORDER BY count_prescription) AS count_prescription_cumsum_by_yj,
      SUM(count_prescription) OVER(PARTITION BY dispensing_date) AS count_prescription_sum_by_yj,
      SUM(sum_total_dose) OVER(PARTITION BY store_code, dispensing_date ORDER BY sum_total_dose) AS sum_total_dose_cumsum_by_yj_store,
      SUM(sum_total_dose) OVER(PARTITION BY store_code, dispensing_date) AS sum_total_dose_sum_by_yj_store,
      SUM(count_prescription) OVER(PARTITION BY store_code, dispensing_date ORDER BY count_prescription) AS count_prescription_cumsum_by_yj_store,
      SUM(count_prescription) OVER(PARTITION BY store_code, dispensing_date) AS count_prescription_sum_by_yj_store,
    FROM PRES_COUNT
  )
)
SELECT
  *
FROM (
  SELECT
    yj_code, 
    store_code,
    -- 予測に使われる日付に修正
    DATE_ADD(dispensing_date, INTERVAL {{sum_days - 1}} DAY) AS dispensing_date,
    sum_total_dose,
    count_prescription,
    total_dose_rank_by_yj,
    total_dose_rank_by_yj_store,
    count_rank_by_yj,
    count_rank_by_yj_store,
    CASE
      WHEN count_rank_by_yj >= 0.3 then 'a'
      WHEN count_rank_by_yj < 0.3 and count_rank_by_yj >= 0.1 then 'b'
      else 'c'
    end as model_abc_flag,
    CASE
      WHEN count_rank_by_yj_store >= 0.3 then 'a'
      WHEN count_rank_by_yj_store < 0.3 and count_rank_by_yj_store >= 0.1 then 'b'
      else 'c'
    end as data_abc_flag,
  FROM PRES_COUNT
  LEFT JOIN DOSE_RANK USING(yj_code, store_code, dispensing_date)
)
WHERE
  dispensing_date >= START_DATE AND dispensing_date < END_DATE