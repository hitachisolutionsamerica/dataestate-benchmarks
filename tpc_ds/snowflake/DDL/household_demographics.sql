create table household_demographics 
(
    hd_demo_sk                int                           ,
    hd_income_band_sk         int                           ,
    hd_buy_potential          varchar(15)                      ,
    hd_dep_count              int                           ,
    hd_vehicle_count          int                            
) 

 CLUSTER BY(hd_demo_sk)
