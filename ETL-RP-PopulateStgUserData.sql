--
--  ETL-RP-PopulateStgUserData
--
--  ETL query to populate the 'stg_user' table
--  stg_user table is a pivot from Account_Id centric to a User_ID centric view of the Account data.
--
--    Created:   Rod Pell
--

INSERT INTO `analytics-take-home-test.Monzo_TakeHome_RP.stg_user` 
  (user_id_hashed, first_account_create_date, signup_cohort, age, gender, address, audit_insert_ts, audit_update_ts)
SELECT user_id_hashed
      ,cast(min(created_ts) as DATE)                    as first_account_create_date
      ,concat(extract(year from min(created_ts))
              ,'-'
              ,case when extract(month from min(created_ts)) in (1,2,3) then 'Q1'
                    when extract(month from min(created_ts)) in (4,5,6) then 'Q2'
                    when extract(month from min(created_ts)) in (7,8,9) then 'Q3'
                    when extract(month from min(created_ts)) in (10,11,12) then 'Q4'
                    else 'Q0' end
              ) as sighup_cohort 
      ,extract(day from min(created_ts)) + 10           as age
      --
      -- Mock up Gender and Address data:
      --
      ,case when upper(substring(user_id_hashed,0,1)) in ('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q') then 'Female'
            else 'Male' end                             as gender
      ,case when upper(substring(account_id_hashed,0,1)) in ('A','B','C','D','E','F','G','H','I','J','K','L','M') then 'London'
            when upper(substring(account_id_hashed,0,1)) in ('N','O','P','Q','R','S','T','U','V','W','X','Y','Z') then 'Brighton'
            else 'Manchester' end                       as address
      ,current_timestamp()                              as audit_insert_ts
      ,current_timestamp()                              as audit_update_ts
FROM `analytics-take-home-test.monzo_datawarehouse.account_created` 
group by user_id_hashed
        ,case when upper(substring(user_id_hashed,0,1)) in ('A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q') then 'Female'
              else 'Male' end   
        ,case when upper(substring(account_id_hashed,0,1)) in ('A','B','C','D','E','F','G','H','I','J','K','L','M') then 'London'
              when upper(substring(account_id_hashed,0,1)) in ('N','O','P','Q','R','S','T','U','V','W','X','Y','Z') then 'Brighton'
              else 'Manchester' end 
