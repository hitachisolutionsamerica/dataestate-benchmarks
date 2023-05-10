create table web_page 
(
    wp_web_page_sk            int                           ,
    wp_web_page_id            varchar(16)                      ,
    wp_rec_start_date         date                          ,
    wp_rec_end_date           date                          ,
    wp_creation_date_sk       int                           ,
    wp_access_date_sk         int                           ,
    wp_autogen_flag           varchar(1)                       ,
    wp_customer_sk            int                           ,
    wp_url                    varchar(100)                  ,
    wp_type                   varchar(50)                      ,
    wp_char_count             int                           ,
    wp_link_count             int                           ,
    wp_image_count            int                           ,
    wp_max_ad_count           int                            
) 

 CLUSTER BY(wp_web_page_sk)
