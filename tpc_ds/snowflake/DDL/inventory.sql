create table inventory 
(
    inv_date_sk               int                           ,
    inv_item_sk               int                           ,
    inv_warehouse_sk          int                           ,
    inv_quantity_on_hand      int                            
) 

 CLUSTER BY(inv_date_sk)
