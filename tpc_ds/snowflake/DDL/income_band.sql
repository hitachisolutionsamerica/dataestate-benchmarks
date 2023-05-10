create table income_band 
(
    ib_income_band_sk         int                           ,
    ib_lower_bound            int                           ,
    ib_upper_bound            int                            
) 

 CLUSTER BY(ib_income_band_sk)
