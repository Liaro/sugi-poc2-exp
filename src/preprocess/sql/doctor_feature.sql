/*
どのdoctorがどのstoreでどのyj_codeを処方したことあるかの特徴
*/
DECLARE END_DATE DATE DEFAULT DATE_SUB(DATE("{{end_ts}}", "Asia/Tokyo"), INTERVAL {{sum_days - 1}} DAY);
DECLARE TRAIN_START_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL {{train_days + valid_days + test_days + 2 * sum_days}} DAY);
DECLARE TRAIN_END_DATE DATE DEFAULT DATE_SUB(END_DATE, INTERVAL {{valid_days + test_days + 2 * sum_days}} DAY);

CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.{{script_name}}`

AS

-- main
SELECT 
  yj_code,
  store_code,
  -- 訓練期間の途中では、未来の情報を使うことになるが、テスト期間でも同じカテゴリを使うため問題ない
  STRING_AGG(doctor_code, ' ') AS doctors
FROM `{{project_id}}.import.t_prescription` 
WHERE
  -- 訓練期間以降は分布が変わるため
  dispensing_date >= TRAIN_START_DATE AND dispensing_date < TRAIN_END_DATE
GROUP BY yj_code, store_code        