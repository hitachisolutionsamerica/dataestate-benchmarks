create table ship_mode 
(
    sm_ship_mode_sk           int                           ,
    sm_ship_mode_id           varchar(16)                      ,
    sm_type                   varchar(30)                      ,
    sm_code                   varchar(10)                      ,
    sm_carrier                varchar(20)                      ,
    sm_contract               varchar(20)                       
) 

 CLUSTER BY(sm_ship_mode_sk)
