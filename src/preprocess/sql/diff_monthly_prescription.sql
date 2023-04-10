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
    total_price_by_yj FLOAT64 OPTIONS(description="yj_code単位の(当日含め)未来{{sum_days}}日分の販売額"),
    lag_total_dose_by_yj_store FLOAT64 OPTIONS(description="yj_code, store_code単位の(当日含め)未来{{sum_days}}日分の処方量の{{sum_days}}日前の値"), 
    lag_total_dose_by_yj FLOAT64 OPTIONS(description="yj_code単位の(当日含め)未来{{sum_days}}日分の処方量の{{sum_days}}日前の値"), 
    diff_total_dose_by_yj_store FLOAT64 OPTIONS(description="yj_code, store_code単位の(当日含め)未来{{sum_days}}日分の処方量の差分"), 
    diff_total_dose_by_yj FLOAT64 OPTIONS(description="yj_code単位の(当日含め)未来{{sum_days}}日分の処方量の差分")
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

SELECT
  *,
  total_dose_by_yj_store - lag_total_dose_by_yj_store as diff_total_dose_by_yj_store,
  total_dose_by_yj - lag_total_dose_by_yj as diff_total_dose_by_yj,
FROM (
  SELECT
    *,
    lag(total_dose_by_yj_store, {{sum_days - 1}}) over(partition by yj_code, store_code order by dispensing_date) as lag_total_dose_by_yj_store,
    lag(total_dose_by_yj, {{sum_days - 1}}) over(partition by yj_code, store_code order by dispensing_date) as lag_total_dose_by_yj,
  FROM `{{project_id}}.{{dataset_id}}.monthly_prescription`
)

;


-- 重複チェック
ASSERT (
SELECT
  COUNT(DISTINCT CONCAT(yj_code, store_code, dispensing_date)) = COUNT(*)
FROM `{{project_id}}.{{dataset_id}}.{{script_name}}`
WHERE
  dispensing_date >= START_DATE AND dispensing_date < DATE_SUB(END_DATE, INTERVAL {{sum_days}} DAY)
) AS "yj_code, store_code, dispensing_date is not unique";
