--
--  ETL_RP_PopulateMeasuresData
--
--  Populate the dm_measures table with all valid combos of measures
--
--     Created:  Rod Pell
--

INSERT INTO `analytics-take-home-test.Monzo_TakeHome_RP.dm_measures` 
    (measures_id, user_signup_cohort, user_age, user_gender, user_address, audit_insert_ts, audit_update_ts)
SELECT row_number() over()      as measures_id 
      ,signup_cohort
      ,age
      ,gender
      ,address
      ,current_timestamp()           as insert_audit_datetime
      ,current_timestamp()           as update_audit_datetime
FROM `analytics-take-home-test.Monzo_TakeHome_RP.stg_user` 
group by signup_cohort
      ,age
      ,gender
      ,address
