/*
  importテーブルから必要なテーブルをまとめて、処方量をyj_code, store_codeごとにまとめたもの。
  処方量は{{sum_days}}日分だけ未来の値まで積算されている。
  統計量の値に必要な期間まで毎回作成し直す
*/


DECLARE START_DATE DATE DEFAULT DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{train_days + valid_days + test_days + 3 * sum_days - 1 + (preceding_days | max)}} DAY);
DECLARE END_DATE DATE DEFAULT DATE("{{end_ts}}", "Asia/Tokyo");


CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.{{script_name}}` 
PARTITION BY dispensing_date
CLUSTER BY store_code
OPTIONS(description="""
  患者ごとの処方遷移。hi_kaisuを使って予定来店日を作成する
""")

AS

WITH DRUG_DATA AS (
  SELECT
    DATE(application_start_date) AS application_start_date,
    yj_code,
    yakushu_zaigata_kubun_id,
    general_name,
    kikaku,
    generic_kubun_id,
    -- 正規化
    LOWER(tanni_name) AS tanni_name,
    tanni_suryo,
    direct_sales_input_kubun_id,
    tokutei_hoken_kubun_id,
    keiryo_kongo_kanou_kubun_id,
    kekkaku_yobou_flag,
    high_risk_drug_flag,
    dokuyakau_gekiyaku_kubun_id,
    kisei_drug_kubun_id,
  FROM
    `{{project_id}}.import.m_drug_latest`
), FILTERED_PRESCRIOTION AS (
  SELECT
    -- カテゴリ
    yj_code,
    lpad(store_code, 6, '0') as store_code,
    -- 時刻
    dispensing_date,
    -- 患者情報
    patient_no,
    -- 処方日数から計算される次回の来店日
    sum(if(hi_kaisu = 0, null, hi_kaisu)) AS hi_kaisu,
    -- 処方薬情報
    sum(total_dose) as total_dose,
    sum(drug_price) as total_price,
  FROM
    `{{project_id}}.import.t_prescription`
  WHERE
    dispensing_date BETWEEN START_DATE AND END_DATE
  group by 1, 2, 3, 4
), YJ_STORE_TEMPLATE AS (
  -- 全ての日付とカテゴリが存在することが保証されているテンプレートを作成
  SELECT DISTINCT
    yj_code,
    lpad(soshiki_unit_code, 6, '0') AS store_code,
  FROM `{{project_id}}.import.m_srq_latest` 
  LEFT JOIN (SELECT yj_id, yj_code FROM `{{project_id}}.import.m_drug_latest`) 
  USING(yj_id)
  LEFT JOIN (SELECT soshiki_unit_id AS soshiki_unit_id_store, soshiki_unit_code FROM `{{project_id}}.import.m_soshikiunit_latest` )
  USING(soshiki_unit_id_store)
), PATIENT_TEMPLATE AS (
  SELECT
    patient_no,
    yj_code,
    store_code,
    dispensing_date,
  FROM (
    SELECT
      patient_no,
      yj_code,
      store_code,
      -- 最初の処方が予測できるかはデータセットとして必要なので、多めに作成
      GREATEST(DATE_SUB(min(dispensing_date), interval 30 day), START_DATE) as min_date,
      -- 最後の処方から200日は処方があるかもしれない想定
      LEAST(DATE_ADD(max(dispensing_date), interval 200 day), END_DATE) as max_date,
    FROM FILTERED_PRESCRIOTION
    JOIN YJ_STORE_TEMPLATE USING(yj_code, store_code)
    GROUP BY 1, 2, 3
  ), UNNEST(GENERATE_DATE_ARRAY(MIN_DATE, MAX_DATE, INTERVAL 1 DAY)) as dispensing_date
)

  -- main
  SELECT 
    patient_no,
    yj_code,
    store_code,
    dispensing_date,
    date_add(dispensing_date, interval hi_kaisu day) as pred_prescription_date,
    ifnull(total_dose, 0) AS total_dose,
    ifnull(total_price, 0) AS total_price,
    drug.* except(application_start_date, yj_code)
  FROM PATIENT_TEMPLATE AS temp
  LEFT JOIN FILTERED_PRESCRIOTION AS prescription
  USING (yj_code, store_code, patient_no, dispensing_date)
  LEFT JOIN DRUG_DATA AS drug
  USING (yj_code) 
;

-- 重複チェック
ASSERT (
SELECT
  COUNT(DISTINCT CONCAT(patient_no, '-', yj_code, '-', store_code, '-', dispensing_date)) = COUNT(*)
FROM `{{project_id}}.{{dataset_id}}.{{script_name}}`
) AS "patient_no, yj_code, store_code, dispensing_date is not unique";
