MERGE INTO `datam_common_dataset.dim_insurance_company` t
USING(
  WITH DIP AS
  (
    SELECT * FROM (
      SELECT
        CAST(CONCAT(CAST(UNIX_SECONDS(CURRENT_TIMESTAMP()) AS STRING)
                    ,CAST((1000 + ROW_NUMBER() OVER ()) AS STRING)) AS INT64) AS insurance_company_key,
        key.id AS insurance_company_id,
        name AS insurance_company_name,
        take_payment,
        updated_at,
        ROW_NUMBER() OVER (PARTITION BY key.id ORDER BY updated_at DESC) AS current_flag,
        CAST('#load_dt#' AS timestamp) AS start_date_ts,
        CAST('2099-12-31 23:59:59' AS timestamp) AS end_date_ts,
        CURRENT_TIMESTAMP() AS etl_load_date_ts
        FROM `events_dev_dataset.monolith_insurance_company_v*`
      WHERE
        EXTRACT(date FROM event_created_timestamp) > date '#prev_load_dt#'
        AND EXTRACT(date FROM event_created_timestamp) <= date '#load_dt#'
    ) i WHERE i.current_flag = 1
  ),
  DAP AS
  (
    SELECT
      insurance_company_key,
      insurance_company_id,
      insurance_company_name,
      take_payment,
      current_flag,
      start_date_ts,
      end_date_ts,
      etl_load_date_ts
      FROM `datam_common_dataset.dim_insurance_company`
    WHERE
      current_flag = TRUE
  )
  SELECT DIP.insurance_company_key, DIP.insurance_company_id, DIP.insurance_company_name, DIP.take_payment, TRUE AS current_flag, DIP.start_date_ts,
    DIP.end_date_ts, DIP.etl_load_date_ts FROM DIP
    WHERE
    NOT EXISTS(SELECT 1 FROM DAP
      WHERE DIP.insurance_company_id = DAP.insurance_company_id
      AND DIP.insurance_company_name = DAP.insurance_company_name
      AND DIP.take_payment = DAP.take_payment)
  UNION ALL
  SELECT DAP.insurance_company_key, DAP.insurance_company_id, DIP.insurance_company_name, DIP.take_payment,
    FALSE AS current_flag, DAP.start_date_ts,
    DIP.start_date_ts AS end_date_ts, DAP.etl_load_date_ts FROM DAP JOIN DIP ON DIP.insurance_company_id = DAP.insurance_company_id
    WHERE
    NOT EXISTS(SELECT 1 FROM DIP
      WHERE DIP.insurance_company_id = DAP.insurance_company_id
      AND DIP.insurance_company_name = DAP.insurance_company_name
      AND DIP.take_payment = DAP.take_payment)

)AS t2 ON t2.insurance_company_id =  t.insurance_company_id AND t.current_flag = TRUE AND t2.current_flag = FALSE
WHEN NOT MATCHED
  THEN
    INSERT (insurance_company_key, insurance_company_id, insurance_company_name, take_payment, current_flag,
      start_date_ts, end_date_ts, etl_load_date_ts, run_id)
    VALUES(t2.insurance_company_key, t2.insurance_company_id, t2.insurance_company_name, t2.take_payment, t2.current_flag,
      t2.start_date_ts, t2.end_date_ts, t2.etl_load_date_ts, #run_id#)

WHEN MATCHED
  THEN
    UPDATE SET current_flag = t2.current_flag, end_date_ts = t2.end_date_ts
