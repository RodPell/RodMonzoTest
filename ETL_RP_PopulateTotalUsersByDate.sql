--
--  ETL_RP_PopulateTotalUsersByDate
--
--  Populate 'stg_total_users_by_date' table with pre-canned 'total users' metrics.
--  
--  Note 1:
--    In reality, this ETL would use the account_status_history table proposed as part of my answer to the first question.
--    However, I present this alternative - confident that this is sufficient for a proof of concept type exercise.
--    (Generally focusing on the data modellng aspects of the test over data engineering)
--    I am aware the final step logic only works due to there being a 1:1 relationship between user_id and account_id in the dataset.
--
--    Please see page "C.4 Next Steps" in the documentation for more info.
--
--  Note 2:
--    I tried this as a WHILE loop, running through all dates in range (~3.5 years).
--    It was taking AGES.  I cancelled the job after 5 mins and nothing produced.
--
--    Hence I ran it manually 20 times - for my sample 20 days  01-Feb-2020 - 20-Feb-2020
--    This also massively reduces the load on BigQuery - and therefore any costs.
--
--     Created:  Rod Pell
--

declare processing_date date default '2020-02-20';

-- Insert row and compute total active accounts
insert into `analytics-take-home-test.Monzo_TakeHome_RP.stg_total_users_by_date`
   (effective_date, measures_id, total_created_accounts, total_closed_accounts, total_reopened_accounts, net_active_users, audit_insert_ts, audit_update_ts)
select processing_date    as effective_date
     , dmm.measures_id     
     , count(distinct(ac.user_id_hashed))    as total_created_accounts
     , 0                  as total_closed_accounts
     , 0                  as total_reopened_accounts
     , 0                  as net_active_users
     , current_timestamp()
     , current_timestamp()
FROM `analytics-take-home-test.monzo_datawarehouse.account_created`  ac
JOIN `analytics-take-home-test.Monzo_TakeHome_RP.stg_user` usr
  on usr.user_id_hashed = ac.user_id_hashed
JOIN `analytics-take-home-test.Monzo_TakeHome_RP.dm_measures` dmm
  on usr.signup_cohort = dmm.user_signup_cohort
 and usr.age = dmm.user_age
 and usr.gender = dmm.user_gender
 and usr.address = dmm.user_address
where cast(created_ts as DATE) <= processing_date
group by processing_date
      , dmm.measures_id;
 
 
-- Calculate total closed accounts
delete from `analytics-take-home-test.Monzo_TakeHome_RP.stg_tmp_closed_totals` where 1=1;

insert into `analytics-take-home-test.Monzo_TakeHome_RP.stg_tmp_closed_totals`
  (effective_date, measures_id, closed_total)
select processing_date    as effective_date
     , dmm.measures_id    as measures_id
     , count(distinct(ac.user_id_hashed))    as closed_total
FROM `analytics-take-home-test.monzo_datawarehouse.account_created`  ac
JOIN `analytics-take-home-test.monzo_datawarehouse.account_closed`  close
  on close.account_id_hashed = ac.account_id_hashed
JOIN `analytics-take-home-test.Monzo_TakeHome_RP.stg_user` usr
  on usr.user_id_hashed = ac.user_id_hashed
JOIN `analytics-take-home-test.Monzo_TakeHome_RP.dm_measures` dmm
  on usr.signup_cohort = dmm.user_signup_cohort
 and usr.age = dmm.user_age
 and usr.gender = dmm.user_gender
 and usr.address = dmm.user_address
where cast(created_ts as DATE) <= processing_date
group by processing_date
      , dmm.measures_id;

update `analytics-take-home-test.Monzo_TakeHome_RP.stg_total_users_by_date` tubd
   set tubd.total_closed_accounts = tmp.closed_total
from `analytics-take-home-test.Monzo_TakeHome_RP.stg_tmp_closed_totals` tmp
where tubd.effective_date = tmp.effective_date
  and tubd.measures_id = tmp.measures_id;


-- Calculate total reopened accounts
delete from `analytics-take-home-test.Monzo_TakeHome_RP.stg_tmp_reopened_totals` where 1=1;

insert into `analytics-take-home-test.Monzo_TakeHome_RP.stg_tmp_reopened_totals`
  (effective_date, measures_id, reopened_total)
select processing_date    as effective_date
     , dmm.measures_id    as measures_id
     , count(distinct(ac.user_id_hashed))    as reopened_total        -- count(distinct)) resolves dulicate closed account_ids
FROM `analytics-take-home-test.monzo_datawarehouse.account_created`  ac
JOIN `analytics-take-home-test.monzo_datawarehouse.account_reopened`  reop
  on reop.account_id_hashed = ac.account_id_hashed
JOIN `analytics-take-home-test.Monzo_TakeHome_RP.stg_user` usr
  on usr.user_id_hashed = ac.user_id_hashed
JOIN `analytics-take-home-test.Monzo_TakeHome_RP.dm_measures` dmm
  on usr.signup_cohort = dmm.user_signup_cohort
 and usr.age = dmm.user_age
 and usr.gender = dmm.user_gender
 and usr.address = dmm.user_address
where cast(created_ts as DATE) <= processing_date
group by processing_date
      , dmm.measures_id;

update `analytics-take-home-test.Monzo_TakeHome_RP.stg_total_users_by_date` tubd
   set tubd.total_reopened_accounts = tmp.reopened_total
from `analytics-take-home-test.Monzo_TakeHome_RP.stg_tmp_reopened_totals` tmp
where tubd.effective_date = tmp.effective_date
  and tubd.measures_id = tmp.measures_id;


-- Calculate net active users
update `analytics-take-home-test.Monzo_TakeHome_RP.stg_total_users_by_date`
   set net_active_users = total_created_accounts - total_closed_accounts + total_reopened_accounts
where effective_date = processing_date
