-- SHOW WAREHOUSES;
ALTER WAREHOUSE WH_2XL resume
;

/* -----------------------
        START - sf30000
----------------------- */

/* DO NOT RERUN CREATE STORAGE INTEGRATION - it will break the S3 trust relationship
    > SF changes the external ID every time this command is run

-- DESC INTEGRATION s3_int;
CREATE STORAGE INTEGRATION s3_int 
  TYPE = external_stage storage_provider = s3 
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/mysnowflakerole' 
  STORAGE_ALLOWED_LOCATIONS = ('s3://<<my-tpcds-data-bucket>>/');
*/

GRANT USAGE ON INTEGRATION s3_int TO ROLE accountadmin
;
/* -----------------------
      TPC-DS SETUP 
----------------------- */

-- SHOW DATABASES;
CREATE DATABASE tpcds_sf30000;
USE tpcds_sf30000;

-- SHOW SCHEMAS;
GRANT CREATE STAGE ON SCHEMA public TO ROLE accountadmin;
USE SCHEMA tpcds_sf30000.public;

-- 
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = csv
  FIELD_DELIMITER = '|'
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = true
  error_on_column_count_mismatch=false
;
CREATE OR REPLACE STAGE tpcds_stage 
  STORAGE_INTEGRATION = s3_int 
  URL = 's3://<<my-tpcds-data-bucket>>/tpcds/2023-01-10/tpcds-2.13/tpcds_sf30000_text/' 
  FILE_FORMAT = csv_format;

-- SHOW TABLES IN tpcds_sf30000.public;
DROP TABLE IF EXISTS tpcds_sf30000.public.call_center           ;
DROP TABLE IF EXISTS tpcds_sf30000.public.catalog_page          ;
DROP TABLE IF EXISTS tpcds_sf30000.public.catalog_returns       ;
DROP TABLE IF EXISTS tpcds_sf30000.public.catalog_sales         ;
DROP TABLE IF EXISTS tpcds_sf30000.public.customer_address      ;
DROP TABLE IF EXISTS tpcds_sf30000.public.customer_demographics ;
DROP TABLE IF EXISTS tpcds_sf30000.public.customer              ;
DROP TABLE IF EXISTS tpcds_sf30000.public.date_dim              ;
DROP TABLE IF EXISTS tpcds_sf30000.public.household_demographics;
DROP TABLE IF EXISTS tpcds_sf30000.public.income_band           ;
DROP TABLE IF EXISTS tpcds_sf30000.public.inventory             ;
DROP TABLE IF EXISTS tpcds_sf30000.public.item                  ;
DROP TABLE IF EXISTS tpcds_sf30000.public.promotion             ;
DROP TABLE IF EXISTS tpcds_sf30000.public.reason                ;
DROP TABLE IF EXISTS tpcds_sf30000.public.ship_mode             ;
DROP TABLE IF EXISTS tpcds_sf30000.public.store_returns         ;
DROP TABLE IF EXISTS tpcds_sf30000.public.store_sales           ;
DROP TABLE IF EXISTS tpcds_sf30000.public.store                 ;
DROP TABLE IF EXISTS tpcds_sf30000.public.time_dim              ;
DROP TABLE IF EXISTS tpcds_sf30000.public.warehouse             ;
DROP TABLE IF EXISTS tpcds_sf30000.public.web_page              ;
DROP TABLE IF EXISTS tpcds_sf30000.public.web_returns           ;
DROP TABLE IF EXISTS tpcds_sf30000.public.web_sales             ;
DROP TABLE IF EXISTS tpcds_sf30000.public.web_site              ;

CREATE TABLE tpcds_sf30000.public.call_center             LIKE snowflake_sample_data.tpcds_sf10tcl.call_center;            
CREATE TABLE tpcds_sf30000.public.catalog_page            LIKE snowflake_sample_data.tpcds_sf10tcl.catalog_page;           
CREATE TABLE tpcds_sf30000.public.catalog_returns         LIKE snowflake_sample_data.tpcds_sf10tcl.catalog_returns;        
CREATE TABLE tpcds_sf30000.public.catalog_sales           LIKE snowflake_sample_data.tpcds_sf10tcl.catalog_sales;          
CREATE TABLE tpcds_sf30000.public.customer_address        LIKE snowflake_sample_data.tpcds_sf10tcl.customer_address;       
CREATE TABLE tpcds_sf30000.public.customer_demographics   LIKE snowflake_sample_data.tpcds_sf10tcl.customer_demographics;  
CREATE TABLE tpcds_sf30000.public.customer                LIKE snowflake_sample_data.tpcds_sf10tcl.customer;               
CREATE TABLE tpcds_sf30000.public.date_dim                LIKE snowflake_sample_data.tpcds_sf10tcl.date_dim;               
CREATE TABLE tpcds_sf30000.public.household_demographics  LIKE snowflake_sample_data.tpcds_sf10tcl.household_demographics; 
CREATE TABLE tpcds_sf30000.public.income_band             LIKE snowflake_sample_data.tpcds_sf10tcl.income_band;            
CREATE TABLE tpcds_sf30000.public.inventory               LIKE snowflake_sample_data.tpcds_sf10tcl.inventory;              
CREATE TABLE tpcds_sf30000.public.item                    LIKE snowflake_sample_data.tpcds_sf10tcl.item;                   
CREATE TABLE tpcds_sf30000.public.promotion               LIKE snowflake_sample_data.tpcds_sf10tcl.promotion;              
CREATE TABLE tpcds_sf30000.public.reason                  LIKE snowflake_sample_data.tpcds_sf10tcl.reason;                 
CREATE TABLE tpcds_sf30000.public.ship_mode               LIKE snowflake_sample_data.tpcds_sf10tcl.ship_mode;              
CREATE TABLE tpcds_sf30000.public.store_returns           LIKE snowflake_sample_data.tpcds_sf10tcl.store_returns;          
CREATE TABLE tpcds_sf30000.public.store_sales             LIKE snowflake_sample_data.tpcds_sf10tcl.store_sales;            
CREATE TABLE tpcds_sf30000.public.store                   LIKE snowflake_sample_data.tpcds_sf10tcl.store;                  
CREATE TABLE tpcds_sf30000.public.time_dim                LIKE snowflake_sample_data.tpcds_sf10tcl.time_dim;               
CREATE TABLE tpcds_sf30000.public.warehouse               LIKE snowflake_sample_data.tpcds_sf10tcl.warehouse;              
CREATE TABLE tpcds_sf30000.public.web_page                LIKE snowflake_sample_data.tpcds_sf10tcl.web_page;               
CREATE TABLE tpcds_sf30000.public.web_returns             LIKE snowflake_sample_data.tpcds_sf10tcl.web_returns;            
CREATE TABLE tpcds_sf30000.public.web_sales               LIKE snowflake_sample_data.tpcds_sf10tcl.web_sales;              
CREATE TABLE tpcds_sf30000.public.web_site                LIKE snowflake_sample_data.tpcds_sf10tcl.web_site;               

COPY INTO tpcds_sf30000.public.call_center            FROM @tpcds_stage/call_center            PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.catalog_page           FROM @tpcds_stage/catalog_page           PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.catalog_returns        FROM @tpcds_stage/catalog_returns        PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.catalog_sales          FROM @tpcds_stage/catalog_sales          PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.customer               FROM @tpcds_stage/customer/              PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.customer_address       FROM @tpcds_stage/customer_address       PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.customer_demographics  FROM @tpcds_stage/customer_demographics  PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.date_dim               FROM @tpcds_stage/date_dim               PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.household_demographics FROM @tpcds_stage/household_demographics PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.income_band            FROM @tpcds_stage/income_band            PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.inventory              FROM @tpcds_stage/inventory              PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.item                   FROM @tpcds_stage/item                   PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.promotion              FROM @tpcds_stage/promotion              PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.reason                 FROM @tpcds_stage/reason                 PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.ship_mode              FROM @tpcds_stage/ship_mode              PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.store                  FROM @tpcds_stage/store/                 PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.store_returns          FROM @tpcds_stage/store_returns          PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.store_sales            FROM @tpcds_stage/store_sales            PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.time_dim               FROM @tpcds_stage/time_dim               PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.warehouse              FROM @tpcds_stage/warehouse              PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.web_page               FROM @tpcds_stage/web_page               PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.web_returns            FROM @tpcds_stage/web_returns            PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.web_sales              FROM @tpcds_stage/web_sales              PATTERN = '.*txt';
COPY INTO tpcds_sf30000.public.web_site               FROM @tpcds_stage/web_site               PATTERN = '.*txt';

/* -----------------------
      ETL PREPARATION
----------------------- */

ALTER SESSION SET QUERY_TAG = 'etl sf30000 - prep 0 - step 1 - drop database'  ; DROP DATABASE IF EXISTS etlprep_sf30000 CASCADE;
ALTER SESSION SET QUERY_TAG = 'etl sf30000 - prep 0 - step 2 - create database'; CREATE DATABASE etlprep_sf30000;
ALTER SESSION SET QUERY_TAG = 'etl sf30000 - prep 0 - step 3 - use database'   ; USE etlprep_sf30000;
ALTER SESSION SET QUERY_TAG = 'etl sf30000 - prep 0 - step 4 - use schema'     ; USE SCHEMA etlprep_sf30000.public;

ALTER SESSION SET QUERY_TAG = 'etl sf30000 - prep 1 - create table - all data';
CREATE TABLE IF NOT EXISTS etlprep_sf30000.public.store_sales_denorm AS
SELECT *
FROM tpcds_sf30000.public.store_sales
   , tpcds_sf30000.public.date_dim
   , tpcds_sf30000.public.time_dim
   , tpcds_sf30000.public.customer
   , tpcds_sf30000.public.customer_demographics
   , tpcds_sf30000.public.household_demographics
   , tpcds_sf30000.public.customer_address
   , tpcds_sf30000.public.store
   , tpcds_sf30000.public.promotion
   , tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
  AND ss_store_sk     = s_store_sk
  AND ss_sold_time_sk = t_time_sk
  AND ss_item_sk      = i_item_sk
  AND ss_customer_sk  = c_customer_sk
  AND ss_cdemo_sk     = cd_demo_sk
  AND ss_hdemo_sk     = hd_demo_sk
  AND ss_addr_sk      = ca_address_sk
  AND ss_promo_sk     = p_promo_sk 
;



ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 1998Q1';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '1998Q1';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 1998Q2';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '1998Q2';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 1998Q3';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '1998Q3';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 1998Q4';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '1998Q4';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 1999Q1';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '1999Q1';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 1999Q2';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '1999Q2';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 1999Q3';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '1999Q3';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 1999Q4';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '1999Q4';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 2000Q1';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '2000Q1';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 2000Q2';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '2000Q2';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 2000Q3';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '2000Q3';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 2000Q4';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '2000Q4';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 2001Q1';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '2001Q1';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 2001Q2';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '2001Q2';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 2001Q3';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '2001Q3';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 2001Q4';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '2001Q4';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 2002Q1';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '2002Q1';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 2002Q2';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '2002Q2';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 2002Q3';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '2002Q3';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 2002Q4';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '2002Q4';
ALTER SESSION
SET QUERY_TAG = 'etl sf30000 - prep 1 - create all data - 2003Q1';
INSERT INTO etlprep_sf30000.public.store_sales_denorm
SELECT *
FROM tpcds_sf30000.public.store_sales,
    tpcds_sf30000.public.date_dim,
    tpcds_sf30000.public.time_dim,
    tpcds_sf30000.public.customer,
    tpcds_sf30000.public.customer_demographics,
    tpcds_sf30000.public.household_demographics,
    tpcds_sf30000.public.customer_address,
    tpcds_sf30000.public.store,
    tpcds_sf30000.public.promotion,
    tpcds_sf30000.public.item
WHERE ss_sold_date_sk = d_date_sk
    AND ss_store_sk = s_store_sk
    AND ss_sold_time_sk = t_time_sk
    AND ss_item_sk = i_item_sk
    AND ss_customer_sk = c_customer_sk
    AND ss_cdemo_sk = cd_demo_sk
    AND ss_hdemo_sk = hd_demo_sk
    AND ss_addr_sk = ca_address_sk
    AND ss_promo_sk = p_promo_sk
    AND D_QUARTER_NAME = '2003Q1';


ALTER SESSION SET QUERY_TAG = 'etl sf30000 - prep 2 - create table - start data';
CREATE TABLE IF NOT EXISTS etlprep_sf30000.public.store_sales_denorm_start         
AS 
SELECT * 
FROM etlprep_sf30000.public.store_sales_denorm 
WHERE MOD(ss_sold_date_sk, 2)  <> 0 
   OR ss_sold_date_sk <= 2452459 
   OR MOD(ss_sold_time_sk, 5) <> 0 
;
ALTER SESSION SET QUERY_TAG = 'etl sf30000 - prep 3 - create table - upsert medium';
CREATE TABLE IF NOT EXISTS etlprep_sf30000.public.store_sales_denorm_upsert        
AS 
SELECT * 
FROM etlprep_sf30000.public.store_sales_denorm 
WHERE MOD(ss_sold_date_sk, 2)  = 0 
  AND MOD(ss_sold_time_sk, 5)   = 0 
  AND ss_sold_date_sk > 2452459 
;
ALTER SESSION SET QUERY_TAG = 'etl sf30000 - prep 4 - create table - insert medium';
CREATE TABLE IF NOT EXISTS etlprep_sf30000.public.store_sales_denorm_insert_medium 
AS 
SELECT * 
FROM etlprep_sf30000.public.store_sales_denorm 
WHERE MOD(ss_sold_date_sk, 2)  = 1 
  AND MOD(ss_sold_time_sk, 25)  = 0 
  AND ss_sold_date_sk > 2452459 
;
ALTER SESSION SET QUERY_TAG = 'etl sf30000 - prep 5 - create table - delete xsmall';
CREATE TABLE IF NOT EXISTS etlprep_sf30000.public.store_sales_denorm_delete_xsmall 
AS 
SELECT * 
FROM etlprep_sf30000.public.store_sales_denorm 
WHERE MOD(ss_sold_date_sk, 10) = 1 
  AND MOD(ss_sold_time_sk, 100) = 0 
  AND ss_sold_date_sk > 2452459 
;
ALTER SESSION SET QUERY_TAG = 'etl sf30000 - prep 6 - create table - delete small';
CREATE TABLE IF NOT EXISTS etlprep_sf30000.public.store_sales_denorm_delete_small  
AS 
SELECT * 
FROM etlprep_sf30000.public.store_sales_denorm 
WHERE MOD(ss_sold_date_sk, 10) = 0 
  AND MOD(ss_sold_time_sk, 10)  = 0 
  AND ss_sold_date_sk > 2452459 
;
ALTER SESSION SET QUERY_TAG = 'etl sf30000 - prep 7 - create table - delete medium';
CREATE TABLE IF NOT EXISTS etlprep_sf30000.public.store_sales_denorm_delete_medium 
AS 
SELECT * 
FROM etlprep_sf30000.public.store_sales_denorm 
WHERE MOD(ss_sold_date_sk, 3)  = 0 
  AND MOD(ss_sold_time_sk, 3)   = 0 
  AND ss_sold_date_sk > 2452459 
;


/* -----------------------
          END
----------------------- */
ALTER WAREHOUSE WH_2XL suspend;

