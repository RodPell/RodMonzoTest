--
--  RP_Query_7dActiveUsersfrom_FT
--
--  Sample query to run on the target model
--

SELECT ft.seven_day_start_date
     , ft.seven_day_end_date
     , dm.user_address
     , (sum(ft.total_7day_active_users) / sum(ft.total_to_date_net_active_users)) * 100   as seven_day_active_users
FROM `analytics-take-home-test.Monzo_TakeHome_RP.ft_7day_activity` ft
JOIN `analytics-take-home-test.Monzo_TakeHome_RP.dm_measures` dm
  ON ft.measures_id = dm.measures_id
WHERE ft.seven_day_end_date = '2020-02-10'
  AND dm.user_age >= 30
GROUP BY ft.seven_day_start_date
     , ft.seven_day_end_date
     , dm.user_address