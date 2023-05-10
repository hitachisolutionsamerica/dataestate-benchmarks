/* -----------------------------------------
       SNOWFLAKE TPC-DS PARQUET LOAD
   Load parquet using SF's insane syntax
----------------------------------------- */

-- SHOW DATABASES;
CREATE DATABASE tpcds_1g;
USE tpcds_1g;

-- SHOW SCHEMAS;
GRANT CREATE STAGE ON SCHEMA public TO ROLE accountadmin;
USE SCHEMA tpcds_1g.public;

-- Create parquet file format (for reasons…)
CREATE OR REPLACE FILE FORMAT parquet_format TYPE = 'parquet';

-- Create a "stage" to reference the S3 location
CREATE OR REPLACE STAGE tpcds_stage 
  STORAGE_INTEGRATION = s3_int 
  URL = 's3://{bucket}/{prefix}/tpcds_sf1_parquet/' 
  FILE_FORMAT = parquet_format;

-- Premptively drop all tables
DROP TABLE IF EXISTS tpcds_1g.public.call_center;            
DROP TABLE IF EXISTS tpcds_1g.public.catalog_page;           
DROP TABLE IF EXISTS tpcds_1g.public.catalog_returns;        
DROP TABLE IF EXISTS tpcds_1g.public.catalog_sales;          
DROP TABLE IF EXISTS tpcds_1g.public.customer;               
DROP TABLE IF EXISTS tpcds_1g.public.customer_address;       
DROP TABLE IF EXISTS tpcds_1g.public.customer_demographics;  
DROP TABLE IF EXISTS tpcds_1g.public.date_dim;               
DROP TABLE IF EXISTS tpcds_1g.public.household_demographics; 
DROP TABLE IF EXISTS tpcds_1g.public.income_band;            
DROP TABLE IF EXISTS tpcds_1g.public.inventory;              
DROP TABLE IF EXISTS tpcds_1g.public.item;                   
DROP TABLE IF EXISTS tpcds_1g.public.promotion;              
DROP TABLE IF EXISTS tpcds_1g.public.reason;                 
DROP TABLE IF EXISTS tpcds_1g.public.ship_mode;              
DROP TABLE IF EXISTS tpcds_1g.public.store;                  
DROP TABLE IF EXISTS tpcds_1g.public.store_returns;          
DROP TABLE IF EXISTS tpcds_1g.public.store_sales;            
DROP TABLE IF EXISTS tpcds_1g.public.time_dim;               
DROP TABLE IF EXISTS tpcds_1g.public.warehouse;              
DROP TABLE IF EXISTS tpcds_1g.public.web_page;               
DROP TABLE IF EXISTS tpcds_1g.public.web_returns;            
DROP TABLE IF EXISTS tpcds_1g.public.web_sales;              
DROP TABLE IF EXISTS tpcds_1g.public.web_site;               

--Create tables by cloning DDL from SF sample tables
CREATE TABLE tpcds_1g.public.call_center            LIKE snowflake_sample_data.tpcds_sf10tcl.call_center;
CREATE TABLE tpcds_1g.public.catalog_page           LIKE snowflake_sample_data.tpcds_sf10tcl.catalog_page;
CREATE TABLE tpcds_1g.public.catalog_returns        LIKE snowflake_sample_data.tpcds_sf10tcl.catalog_returns;
CREATE TABLE tpcds_1g.public.catalog_sales          LIKE snowflake_sample_data.tpcds_sf10tcl.catalog_sales;
CREATE TABLE tpcds_1g.public.customer_address       LIKE snowflake_sample_data.tpcds_sf10tcl.customer_address;
CREATE TABLE tpcds_1g.public.customer_demographics  LIKE snowflake_sample_data.tpcds_sf10tcl.customer_demographics;
CREATE TABLE tpcds_1g.public.customer               LIKE snowflake_sample_data.tpcds_sf10tcl.customer;
CREATE TABLE tpcds_1g.public.date_dim               LIKE snowflake_sample_data.tpcds_sf10tcl.date_dim;
CREATE TABLE tpcds_1g.public.household_demographics LIKE snowflake_sample_data.tpcds_sf10tcl.household_demographics;
CREATE TABLE tpcds_1g.public.income_band            LIKE snowflake_sample_data.tpcds_sf10tcl.income_band;
CREATE TABLE tpcds_1g.public.inventory              LIKE snowflake_sample_data.tpcds_sf10tcl.inventory;
CREATE TABLE tpcds_1g.public.item                   LIKE snowflake_sample_data.tpcds_sf10tcl.item;
CREATE TABLE tpcds_1g.public.promotion              LIKE snowflake_sample_data.tpcds_sf10tcl.promotion;
CREATE TABLE tpcds_1g.public.reason                 LIKE snowflake_sample_data.tpcds_sf10tcl.reason;
CREATE TABLE tpcds_1g.public.ship_mode              LIKE snowflake_sample_data.tpcds_sf10tcl.ship_mode;
CREATE TABLE tpcds_1g.public.store_returns          LIKE snowflake_sample_data.tpcds_sf10tcl.store_returns;
CREATE TABLE tpcds_1g.public.store_sales            LIKE snowflake_sample_data.tpcds_sf10tcl.store_sales;
CREATE TABLE tpcds_1g.public.store                  LIKE snowflake_sample_data.tpcds_sf10tcl.store;
CREATE TABLE tpcds_1g.public.time_dim               LIKE snowflake_sample_data.tpcds_sf10tcl.time_dim;
CREATE TABLE tpcds_1g.public.warehouse              LIKE snowflake_sample_data.tpcds_sf10tcl.warehouse;
CREATE TABLE tpcds_1g.public.web_page               LIKE snowflake_sample_data.tpcds_sf10tcl.web_page;
CREATE TABLE tpcds_1g.public.web_returns            LIKE snowflake_sample_data.tpcds_sf10tcl.web_returns;
CREATE TABLE tpcds_1g.public.web_sales              LIKE snowflake_sample_data.tpcds_sf10tcl.web_sales;
CREATE TABLE tpcds_1g.public.web_site               LIKE snowflake_sample_data.tpcds_sf10tcl.web_site;

-- Load parquet from stage using their INSANE $1 syntax
  -- Note the manual logic needed to parse the partition value into a column 
  -- NULLIF(REGEXP_REPLACE(METADATA$FILENAME,'.*\\=(.*)\\/.*','\\1'),'__HIVE_DEFAULT_PARTITION__')
  -- Borrowed from https://community.snowflake.com/s/article/How-to-Load-Terabytes-Into-Snowflake-Speeds-Feeds-and-Techniques
COPY INTO tpcds_1g.public.call_center            FROM (SELECT $1:cc_call_center_sk, $1:cc_call_center_id, $1:cc_rec_start_date, $1:cc_rec_end_date, $1:cc_closed_date_sk, $1:cc_open_date_sk, $1:cc_name, $1:cc_class, $1:cc_employees, $1:cc_sq_ft, $1:cc_hours, $1:cc_manager, $1:cc_mkt_id, $1:cc_mkt_class, $1:cc_mkt_desc, $1:cc_market_manager, $1:cc_division, $1:cc_division_name, $1:cc_company, $1:cc_company_name, $1:cc_street_number, $1:cc_street_name, $1:cc_street_type, $1:cc_suite_number, $1:cc_city, $1:cc_county, $1:cc_state, $1:cc_zip, $1:cc_country, $1:cc_gmt_offset, $1:cc_tax_percentage FROM @tpcds_stage/call_center) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.catalog_page           FROM (SELECT $1:cp_catalog_page_sk, $1:cp_catalog_page_id, $1:cp_start_date_sk, $1:cp_end_date_sk, $1:cp_department, $1:cp_catalog_number, $1:cp_catalog_page_number, $1:cp_description, $1:cp_type FROM @tpcds_stage/catalog_page) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.catalog_returns        FROM (SELECT NULLIF(REGEXP_REPLACE(METADATA$FILENAME,'.*\\=(.*)\\/.*','\\1'),'__HIVE_DEFAULT_PARTITION__'), $1:cr_returned_time_sk, $1:cr_item_sk, $1:cr_refunded_customer_sk, $1:cr_refunded_cdemo_sk, $1:cr_refunded_hdemo_sk, $1:cr_refunded_addr_sk, $1:cr_returning_customer_sk, $1:cr_returning_cdemo_sk, $1:cr_returning_hdemo_sk, $1:cr_returning_addr_sk, $1:cr_call_center_sk, $1:cr_catalog_page_sk, $1:cr_ship_mode_sk, $1:cr_warehouse_sk, $1:cr_reason_sk, $1:cr_order_number, $1:cr_return_quantity, $1:cr_return_amount, $1:cr_return_tax, $1:cr_return_amt_inc_tax, $1:cr_fee, $1:cr_return_ship_cost, $1:cr_refunded_cash, $1:cr_reversed_charge, $1:cr_store_credit, $1:cr_net_loss FROM @tpcds_stage/catalog_returns) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.catalog_sales          FROM (SELECT NULLIF(REGEXP_REPLACE(METADATA$FILENAME,'.*\\=(.*)\\/.*','\\1'),'__HIVE_DEFAULT_PARTITION__'), $1:cs_sold_time_sk, $1:cs_ship_date_sk, $1:cs_bill_customer_sk, $1:cs_bill_cdemo_sk, $1:cs_bill_hdemo_sk, $1:cs_bill_addr_sk, $1:cs_ship_customer_sk, $1:cs_ship_cdemo_sk, $1:cs_ship_hdemo_sk, $1:cs_ship_addr_sk, $1:cs_call_center_sk, $1:cs_catalog_page_sk, $1:cs_ship_mode_sk, $1:cs_warehouse_sk, $1:cs_item_sk, $1:cs_promo_sk, $1:cs_order_number, $1:cs_quantity, $1:cs_wholesale_cost, $1:cs_list_price, $1:cs_sales_price, $1:cs_ext_discount_amt, $1:cs_ext_sales_price, $1:cs_ext_wholesale_cost, $1:cs_ext_list_price, $1:cs_ext_tax, $1:cs_coupon_amt, $1:cs_ext_ship_cost, $1:cs_net_paid, $1:cs_net_paid_inc_tax, $1:cs_net_paid_inc_ship, $1:cs_net_paid_inc_ship_tax, $1:cs_net_profit FROM @tpcds_stage/catalog_sales) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.customer_address       FROM (SELECT $1:ca_address_sk, $1:ca_address_id, $1:ca_street_number, $1:ca_street_name, $1:ca_street_type, $1:ca_suite_number, $1:ca_city, $1:ca_county, $1:ca_state, $1:ca_zip, $1:ca_country, $1:ca_gmt_offset, $1:ca_location_type FROM @tpcds_stage/customer_address) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.customer_demographics  FROM (SELECT $1:cd_demo_sk, $1:cd_gender, $1:cd_marital_status, $1:cd_education_status, $1:cd_purchase_estimate, $1:cd_credit_rating, $1:cd_dep_count, $1:cd_dep_employed_count, $1:cd_dep_college_count FROM @tpcds_stage/customer_demographics) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.customer               FROM (SELECT $1:c_customer_sk, $1:c_customer_id, $1:c_current_cdemo_sk, $1:c_current_hdemo_sk, $1:c_current_addr_sk, $1:c_first_shipto_date_sk, $1:c_first_sales_date_sk, $1:c_salutation, $1:c_first_name, $1:c_last_name, $1:c_preferred_cust_flag, $1:c_birth_day, $1:c_birth_month, $1:c_birth_year, $1:c_birth_country, $1:c_login, $1:c_email_address, $1:c_last_review_date_sk FROM @tpcds_stage/customer) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.date_dim               FROM (SELECT $1:d_date_sk, $1:d_date_id, $1:d_date, $1:d_month_seq, $1:d_week_seq, $1:d_quarter_seq, $1:d_year, $1:d_dow, $1:d_moy, $1:d_dom, $1:d_qoy, $1:d_fy_year, $1:d_fy_quarter_seq, $1:d_fy_week_seq, $1:d_day_name, $1:d_quarter_name, $1:d_holiday, $1:d_weekend, $1:d_following_holiday, $1:d_first_dom, $1:d_last_dom, $1:d_same_day_ly, $1:d_same_day_lq, $1:d_current_day, $1:d_current_week, $1:d_current_month, $1:d_current_quarter, $1:d_current_year FROM @tpcds_stage/date_dim) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.household_demographics FROM (SELECT $1:hd_demo_sk, $1:hd_income_band_sk, $1:hd_buy_potential, $1:hd_dep_count, $1:hd_vehicle_count FROM @tpcds_stage/household_demographics) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.income_band            FROM (SELECT $1:ib_income_band_sk, $1:ib_lower_bound, $1:ib_upper_bound FROM @tpcds_stage/income_band) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.inventory              FROM (SELECT NULLIF(REGEXP_REPLACE(METADATA$FILENAME,'.*\\=(.*)\\/.*','\\1'),'__HIVE_DEFAULT_PARTITION__'), $1:inv_item_sk, $1:inv_warehouse_sk, $1:inv_quantity_on_hand FROM @tpcds_stage/inventory) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.item                   FROM (SELECT $1:i_item_sk, $1:i_item_id, $1:i_rec_start_date, $1:i_rec_end_date, $1:i_item_desc, $1:i_current_price, $1:i_wholesale_cost, $1:i_brand_id, $1:i_brand, $1:i_class_id, $1:i_class, $1:i_category_id, $1:i_category, $1:i_manufact_id, $1:i_manufact, $1:i_size, $1:i_formulation, $1:i_color, $1:i_units, $1:i_container, $1:i_manager_id, $1:i_product_name FROM @tpcds_stage/item) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.promotion              FROM (SELECT $1:p_promo_sk, $1:p_promo_id, $1:p_start_date_sk, $1:p_end_date_sk, $1:p_item_sk, $1:p_cost, $1:p_response_target, $1:p_promo_name, $1:p_channel_dmail, $1:p_channel_email, $1:p_channel_catalog, $1:p_channel_tv, $1:p_channel_radio, $1:p_channel_press, $1:p_channel_event, $1:p_channel_demo, $1:p_channel_details, $1:p_purpose, $1:p_discount_active FROM @tpcds_stage/promotion) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.reason                 FROM (SELECT $1:r_reason_sk, $1:r_reason_id, $1:r_reason_desc FROM @tpcds_stage/reason) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.ship_mode              FROM (SELECT $1:sm_ship_mode_sk, $1:sm_ship_mode_id, $1:sm_type, $1:sm_code, $1:sm_carrier, $1:sm_contract FROM @tpcds_stage/ship_mode) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.store_returns          FROM (SELECT NULLIF(REGEXP_REPLACE(METADATA$FILENAME,'.*\\=(.*)\\/.*','\\1'),'__HIVE_DEFAULT_PARTITION__'), $1:sr_return_time_sk, $1:sr_item_sk, $1:sr_customer_sk, $1:sr_cdemo_sk, $1:sr_hdemo_sk, $1:sr_addr_sk, $1:sr_store_sk, $1:sr_reason_sk, $1:sr_ticket_number, $1:sr_return_quantity, $1:sr_return_amt, $1:sr_return_tax, $1:sr_return_amt_inc_tax, $1:sr_fee, $1:sr_return_ship_cost, $1:sr_refunded_cash, $1:sr_reversed_charge, $1:sr_store_credit, $1:sr_net_loss FROM @tpcds_stage/store_returns) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.store_sales            FROM (SELECT NULLIF(REGEXP_REPLACE(METADATA$FILENAME,'.*\\=(.*)\\/.*','\\1'),'__HIVE_DEFAULT_PARTITION__'), $1:ss_sold_time_sk, $1:ss_item_sk, $1:ss_customer_sk, $1:ss_cdemo_sk, $1:ss_hdemo_sk, $1:ss_addr_sk, $1:ss_store_sk, $1:ss_promo_sk, $1:ss_ticket_number, $1:ss_quantity, $1:ss_wholesale_cost, $1:ss_list_price, $1:ss_sales_price, $1:ss_ext_discount_amt, $1:ss_ext_sales_price, $1:ss_ext_wholesale_cost, $1:ss_ext_list_price, $1:ss_ext_tax, $1:ss_coupon_amt, $1:ss_net_paid, $1:ss_net_paid_inc_tax, $1:ss_net_profit FROM @tpcds_stage/store_sales) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.store                  FROM (SELECT $1:s_store_sk, $1:s_store_id, $1:s_rec_start_date, $1:s_rec_end_date, $1:s_closed_date_sk, $1:s_store_name, $1:s_number_employees, $1:s_floor_space, $1:s_hours, $1:s_manager, $1:s_market_id, $1:s_geography_class, $1:s_market_desc, $1:s_market_manager, $1:s_division_id, $1:s_division_name, $1:s_company_id, $1:s_company_name, $1:s_street_number, $1:s_street_name, $1:s_street_type, $1:s_suite_number, $1:s_city, $1:s_county, $1:s_state, $1:s_zip, $1:s_country, $1:s_gmt_offset, $1:s_tax_precentage FROM @tpcds_stage/store) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.time_dim               FROM (SELECT $1:t_time_sk, $1:t_time_id, $1:t_time, $1:t_hour, $1:t_minute, $1:t_second, $1:t_am_pm, $1:t_shift, $1:t_sub_shift, $1:t_meal_time FROM @tpcds_stage/time_dim) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.warehouse              FROM (SELECT $1:w_warehouse_sk, $1:w_warehouse_id, $1:w_warehouse_name, $1:w_warehouse_sq_ft, $1:w_street_number, $1:w_street_name, $1:w_street_type, $1:w_suite_number, $1:w_city, $1:w_county, $1:w_state, $1:w_zip, $1:w_country, $1:w_gmt_offset FROM @tpcds_stage/warehouse) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.web_page               FROM (SELECT $1:wp_web_page_sk, $1:wp_web_page_id, $1:wp_rec_start_date, $1:wp_rec_end_date, $1:wp_creation_date_sk, $1:wp_access_date_sk, $1:wp_autogen_flag, $1:wp_customer_sk, $1:wp_url, $1:wp_type, $1:wp_char_count, $1:wp_link_count, $1:wp_image_count, $1:wp_max_ad_count FROM @tpcds_stage/web_page) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.web_returns            FROM (SELECT NULLIF(REGEXP_REPLACE(METADATA$FILENAME,'.*\\=(.*)\\/.*','\\1'),'__HIVE_DEFAULT_PARTITION__'), $1:wr_returned_time_sk, $1:wr_item_sk, $1:wr_refunded_customer_sk, $1:wr_refunded_cdemo_sk, $1:wr_refunded_hdemo_sk, $1:wr_refunded_addr_sk, $1:wr_returning_customer_sk, $1:wr_returning_cdemo_sk, $1:wr_returning_hdemo_sk, $1:wr_returning_addr_sk, $1:wr_web_page_sk, $1:wr_reason_sk, $1:wr_order_number, $1:wr_return_quantity, $1:wr_return_amt, $1:wr_return_tax, $1:wr_return_amt_inc_tax, $1:wr_fee, $1:wr_return_ship_cost, $1:wr_refunded_cash, $1:wr_reversed_charge, $1:wr_account_credit, $1:wr_net_loss FROM @tpcds_stage/web_returns) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.web_sales              FROM (SELECT NULLIF(REGEXP_REPLACE(METADATA$FILENAME,'.*\\=(.*)\\/.*','\\1'),'__HIVE_DEFAULT_PARTITION__'), $1:ws_sold_time_sk, $1:ws_ship_date_sk, $1:ws_item_sk, $1:ws_bill_customer_sk, $1:ws_bill_cdemo_sk, $1:ws_bill_hdemo_sk, $1:ws_bill_addr_sk, $1:ws_ship_customer_sk, $1:ws_ship_cdemo_sk, $1:ws_ship_hdemo_sk, $1:ws_ship_addr_sk, $1:ws_web_page_sk, $1:ws_web_site_sk, $1:ws_ship_mode_sk, $1:ws_warehouse_sk, $1:ws_promo_sk, $1:ws_order_number, $1:ws_quantity, $1:ws_wholesale_cost, $1:ws_list_price, $1:ws_sales_price, $1:ws_ext_discount_amt, $1:ws_ext_sales_price, $1:ws_ext_wholesale_cost, $1:ws_ext_list_price, $1:ws_ext_tax, $1:ws_coupon_amt, $1:ws_ext_ship_cost, $1:ws_net_paid, $1:ws_net_paid_inc_tax, $1:ws_net_paid_inc_ship, $1:ws_net_paid_inc_ship_tax, $1:ws_net_profit FROM @tpcds_stage/web_sales) PATTERN = '.*parquet';
COPY INTO tpcds_1g.public.web_site               FROM (SELECT $1:web_site_sk, $1:web_site_id, $1:web_rec_start_date, $1:web_rec_end_date, $1:web_name, $1:web_open_date_sk, $1:web_close_date_sk, $1:web_class, $1:web_manager, $1:web_mkt_id, $1:web_mkt_class, $1:web_mkt_desc, $1:web_market_manager, $1:web_company_id, $1:web_company_name, $1:web_street_number, $1:web_street_name, $1:web_street_type, $1:web_suite_number, $1:web_city, $1:web_county, $1:web_state, $1:web_zip, $1:web_country, $1:web_gmt_offset, $1:web_tax_percentage FROM @tpcds_stage/web_site) PATTERN = '.*parquet';

SHOW TABLES IN tpcds_1g.public;