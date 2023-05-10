create table reason 
(
    r_reason_sk               int                           ,
    r_reason_id               varchar(16)                      ,
    r_reason_desc             varchar(100)                      
) 

 CLUSTER BY(r_reason_sk)
