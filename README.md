# RodMonzoTest
SQL Scripts created for Rod Pell's Monzo take home test

6 ETL scripts in all:

ETL_RP_Populate7DayActiveUsers -->> Populates the 'stg_7day_active_users_by_date' table
ETL_RP_PopulateFactTable       -->> Populates the 'ft_7day_activity' table
ETL_RP_PopulateMeasuresData    -->> Populates the 'dm_measures' table
ETL_RP_PopulateTotalUsersByDate  -->> Populates the 'stg_total_users_by_date' table
                                      Also uses, and empties, the two 'stg_tmp_xxx' temp tables.
ETL-RP-PopulateStgUserData       -->> Populates the 'stg_user' table, and mocks up age/cohort/gender/address data

And one query script to demo a user using the target tables to pull the 7day_active_users metric
RP_Query_7dActiveUsersfrom_FT
