--
--  ETL_RP_Populate7DayActiveUsers
--
--  Populate 'stg_7day_users_by_date' table with pre-canned '7d users' metrics.
--
--  Note 1:
--     Only populating for the sample date range 01-Feb-2020 -> 20-Feb-2020
--     Converting to a WHILE loop to scan whole data set was taking AGES to process.
--
--     Created:  Rod Pell
--

declare processing_date date default '2020-02-20';

-- Insert row and compute 7day active users
insert into `analytics-take-home-test.Monzo_TakeHome_RP.stg_7day_active_users_by_date`
   (effective_date, effective_date_minus_7, measures_id, total_7day_active_users, audit_insert_ts, audit_update_ts)
select processing_date                            as effective_date
     , date_sub(processing_date, interval 7 day)  as effective_date_minus_7
     , dmm.measures_id     
     , count(distinct(ac.user_id_hashed))         as day7_active_users
     , current_timestamp()
     , current_timestamp()
FROM `analytics-take-home-test.monzo_datawarehouse.account_transactions` txn
JOIN `analytics-take-home-test.monzo_datawarehouse.account_created`  ac
  on txn.account_id_hashed = ac.account_id_hashed
JOIN `analytics-take-home-test.Monzo_TakeHome_RP.stg_user` usr
  on usr.user_id_hashed = ac.user_id_hashed
JOIN `analytics-take-home-test.Monzo_TakeHome_RP.dm_measures` dmm
  on usr.signup_cohort = dmm.user_signup_cohort
 and usr.age = dmm.user_age
 and usr.gender = dmm.user_gender
 and usr.address = dmm.user_address
where txn.date between date_sub(processing_date, interval 6 day) and processing_date
group by processing_date
      , dmm.measures_id
      , date_sub(processing_date, interval 6 day)