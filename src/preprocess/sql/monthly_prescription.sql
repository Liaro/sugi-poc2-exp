/*
  importテーブルから必要なテーブルをまとめて、処方量をyj_code, store_codeごとにまとめたもの。
  処方量は{{sum_days}}日分だけ未来の値まで積算されている。
  統計量の値に必要な期間まで毎回作成し直す
*/


DECLARE START_DATE DATE DEFAULT DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{train_days + valid_days + test_days + 3 * sum_days - 1 + (preceding_days | max)}} DAY);
DECLARE END_DATE DATE DEFAULT DATE("{{end_ts}}", "Asia/Tokyo");


CREATE TABLE IF NOT EXISTS `{{project_id}}.{{dataset_id}}.{{script_name}}` (
    yj_code	STRING NOT NULL OPTIONS(description="YJコード"),
    store_code STRING NOT NULL OPTIONS(description="店舗コード"),
    dispensing_date DATE NOT NULL OPTIONS(description="処方日"),
    dayofweek INT64 NOT NULL OPTIONS(description="週単位の日数"),
    dayofyear INT64 NOT NULL OPTIONS(description="年単位の日数"),
    day	INT64 NOT NULL OPTIONS(description="その月の何日か"),
    week INT64 NOT NULL OPTIONS(description="何週目か"),
    application_start_date DATE OPTIONS(description="適用期間開始日"),
    yakushu_zaigata_kubun_id STRING OPTIONS(description="薬種(剤型)区分ID"), 
    general_name STRING OPTIONS(description="一般名称"), 
    kikaku STRING OPTIONS(description="規格"), 
    generic_kubun_id STRING OPTIONS(description="後発品区分ID"), 
    tanni_name STRING OPTIONS(description="単位名称"), 
    tanni_suryo FLOAT64 OPTIONS(description="単位数量"), 
    direct_sales_input_kubun_id STRING OPTIONS(description="直接販売入力区分ID"), 
    tokutei_hoken_kubun_id STRING OPTIONS(description="特定保険区分ID"), 
    keiryo_kongo_kanou_kubun_id STRING OPTIONS(description="計量混合可能区分ID"), 
    kekkaku_yobou_flag STRING OPTIONS(description="結核予防法フラグ"), 
    high_risk_drug_flag STRING OPTIONS(description="ハイリスク薬フラグ"), 
    dokuyakau_gekiyaku_kubun_id STRING OPTIONS(description="毒薬・劇薬区分ID"), 
    kisei_drug_kubun_id STRING OPTIONS(description="規制医薬品区分ID"), 
    days_from_application INT64 OPTIONS(description="薬の適用開始からの経過日数"), 
    nunique_patient_by_yj_store INT64 OPTIONS(description="yj_code, store_code単位の患者のユニーク数"), 
    nunique_patient_by_yj INT64 OPTIONS(description="yj_code単位の患者のユニーク数"), 
    total_dose_by_yj_store FLOAT64 OPTIONS(description="yj_code, store_code単位の(当日含め)未来{{sum_days}}日分の処方量"), 
    total_dose_by_yj FLOAT64 OPTIONS(description="yj_code単位の(当日含め)未来{{sum_days}}日分の処方量"), 
    total_price_by_yj_store FLOAT64 OPTIONS(description="yj_code, store_code単位の(当日含め)未来{{sum_days}}日分の販売額"), 
    total_price_by_yj FLOAT64 OPTIONS(description="yj_code単位の(当日含め)未来{{sum_days}}日分の販売額")
)
PARTITION BY dispensing_date
CLUSTER BY store_code
OPTIONS(description="""
    importテーブルから必要なテーブルをまとめて、処方量をyj_code, store_codeごとにまとめたもの。
    処方量は{{sum_days}}日分だけ未来の値まで積算されている。
""")
;

DELETE {{dataset_id}}.{{script_name}}
-- 完全なtargetが作成されている期間のみ挿入する
WHERE dispensing_date >= START_DATE AND dispensing_date < DATE_SUB(END_DATE, INTERVAL {{sum_days - 1}} DAY)
;

INSERT {{dataset_id}}.{{script_name}}


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
    -- 処方薬情報
    total_dose,
    drug_price as total_price,
  FROM
    `{{project_id}}.import.t_prescription_view`
  WHERE
    dispensing_date BETWEEN START_DATE AND END_DATE
), TEMPLATE AS (
  -- 全ての日付とカテゴリが存在することが保証されているテンプレートを作成
  SELECT DISTINCT
    yj_code,
    lpad(soshiki_unit_code, 6, '0') AS store_code,
    dispensing_date,
    EXTRACT(DAYOFWEEK FROM dispensing_date) AS dayofweek,
    EXTRACT(DAYOFYEAR FROM dispensing_date) AS dayofyear,
    EXTRACT(DAY FROM dispensing_date) AS day,
    EXTRACT(WEEK FROM dispensing_date) AS week,
  FROM `{{project_id}}.import.m_srq_latest` 
  LEFT JOIN (SELECT yj_id, yj_code FROM `{{project_id}}.import.m_drug_latest`) 
  USING(yj_id)
  LEFT JOIN (SELECT soshiki_unit_id AS soshiki_unit_id_store, soshiki_unit_code FROM `{{project_id}}.import.m_soshikiunit_latest` )
  USING(soshiki_unit_id_store),
  UNNEST(GENERATE_DATE_ARRAY(START_DATE, END_DATE, INTERVAL 1 DAY)) AS dispensing_date
), PRESCRIPTION_GROUP_BY_YJ_STORE AS (
  SELECT
    -- カテゴリ
    yj_code,
    store_code,
    -- 時刻
    dispensing_date,
    -- 特徴として使えそうなもの
    COUNT(DISTINCT patient_no) AS nunique_patient_by_yj_store,
  FROM
    FILTERED_PRESCRIOTION
  GROUP BY
    yj_code, store_code, dispensing_date
), PRESCRIPTION_GROUP_BY_YJ AS (
  SELECT
    -- カテゴリ
    yj_code,
    -- 時刻
    dispensing_date,
    -- ラベル
    -- 特徴として使えそうなもの
    COUNT(DISTINCT patient_no) AS nunique_patient_by_yj,
  FROM
    FILTERED_PRESCRIOTION
  GROUP BY yj_code, dispensing_date
)
SELECT 
  -- WINDOW関数で発生する重複をなくす
  DISTINCT *
FROM (
  -- main
  SELECT 
    temp.*,
    drug.* except(yj_code),
    DATE_DIFF(application_start_date, dispensing_date, DAY) AS days_from_application,
    -- feature
    target_yj_store.nunique_patient_by_yj_store,
    target_yj.nunique_patient_by_yj,
    SUM(IFNULL(prescription.total_dose, 0)) OVER (PARTITION BY yj_code, store_code ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN CURRENT ROW AND {{sum_days - 1}} FOLLOWING )AS total_dose_by_yj_store,
    SUM(IFNULL(prescription.total_dose, 0)) OVER (PARTITION BY yj_code ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN CURRENT ROW AND {{sum_days - 1}} FOLLOWING )AS total_dose_by_yj,
    SUM(IFNULL(prescription.total_price, 0)) OVER (PARTITION BY yj_code, store_code ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN CURRENT ROW AND {{sum_days - 1}} FOLLOWING )AS total_price_by_yj_store,
    SUM(IFNULL(prescription.total_price, 0)) OVER (PARTITION BY yj_code ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN CURRENT ROW AND {{sum_days - 1}} FOLLOWING )AS total_price_by_yj,
    # COUNT(DISTINCT prescription.patient_id) OVER (PARTITION BY yj_code, store_code ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN CURRENT ROW AND {{sum_days - 1}} FOLLOWING )AS nunique_patient_by_yj_store,
    # COUNT(DISTINCT prescription.patient_id) OVER (PARTITION BY yj_code ORDER BY UNIX_DATE(dispensing_date) RANGE BETWEEN CURRENT ROW AND {{sum_days - 1}} FOLLOWING )AS nunique_patient_by_yj,
  FROM TEMPLATE AS temp
  LEFT JOIN FILTERED_PRESCRIOTION AS prescription
  USING (yj_code, store_code, dispensing_date)
  LEFT JOIN PRESCRIPTION_GROUP_BY_YJ_STORE AS target_yj_store
  USING (yj_code, store_code, dispensing_date)
  LEFT JOIN PRESCRIPTION_GROUP_BY_YJ AS target_yj
  USING (yj_code, dispensing_date)
  LEFT JOIN DRUG_DATA AS drug
  USING (yj_code) 
)
-- 完全なtargetが作成されている期間のみ挿入する
WHERE dispensing_date >= START_DATE AND dispensing_date < DATE_SUB(END_DATE, INTERVAL {{sum_days - 1}} DAY)

;

-- 重複チェック
ASSERT (
SELECT
  COUNT(DISTINCT CONCAT(yj_code, store_code, dispensing_date)) = COUNT(*)
FROM `{{project_id}}.{{dataset_id}}.{{script_name}}`
WHERE
  dispensing_date >= START_DATE AND dispensing_date < DATE_SUB(END_DATE, INTERVAL {{sum_days}} DAY)
) AS "yj_code, store_code, dispensing_date is not unique";
