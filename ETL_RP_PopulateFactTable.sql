--
--  ETL_RP_PopulateFactTable
--
--  Build the fact table
--
--  Note 1:
--    Doing this in two steps, firstly as an insert from the 'total_users_by_date' table.
--    Because this table is guarunteed to have data for ALL dates in range, thus is an opportunity to 
--    default in 0 as a 7 day active total for dates with no activity.
--
--  This is in response to the DQ analysis that revealed gaps in the transaction dates.
--
--    Created:  Rod Pell
--

-- Insert rows and default 0 as the 7day active total
INSERT INTO `analytics-take-home-test.Monzo_TakeHome_RP.ft_7day_activity` 
    (seven_day_start_date, seven_day_end_date, measures_id, total_7day_active_users, total_to_date_net_active_users, audit_insert_ts, audit_update_ts)
SELECT date_sub(effective_date, interval 6 day)     as seven_day_start_date
      ,effective_date                               as seven_day_end_date
      ,measures_id
      ,0                                            as total_7day_active_users
      ,net_active_users
      ,current_timestamp()
      ,current_timestamp()
FROM `analytics-take-home-test.Monzo_TakeHome_RP.stg_total_users_by_date`;


-- Update to reflect the correct 7day active totals
UPDATE `analytics-take-home-test.Monzo_TakeHome_RP.ft_7day_activity` ft
   SET ft.total_7day_active_users = stg.total_7day_active_users
FROM `analytics-take-home-test.Monzo_TakeHome_RP.stg_7day_active_users_by_date` stg
WHERE ft.seven_day_end_date = stg.effective_date
  AND ft.measures_id = stg.measures_id