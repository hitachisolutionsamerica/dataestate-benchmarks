CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.RESET_ALL_SP()
  returns string
  language javascript
  as
  $$
  // Purpose: truncate all tables, reset all sequences, and rebuild all streams
  // Truncate all tables
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.CTRL_BATCH"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.LOAD_SNAPSHOT"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.ACCOUNT_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.AUDIT"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.CASHTRANSACTION_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.CUSTOMER_MGMT_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.CUSTOMER_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.CUSTOMER_TEST_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.DAILYMARKET_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.DATE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.FINWIRE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.HOLDINGHISTORY_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.HR_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.INDUSTRY_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.PROSPECT_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.STATUSTYPE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.TAXRATE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.TIME_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.TRADEHISTORY_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.TRADETYPE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.TRADE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_STG.PUBLIC.WATCH_HISTORY_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_ODS.PUBLIC.ACCOUNT_ODS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_ODS.PUBLIC.CUSTOMER_ODS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_ODS.PUBLIC.FINWIRE_CMP_ODS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_ODS.PUBLIC.FINWIRE_FIN_ODS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_ODS.PUBLIC.FINWIRE_SEC_ODS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_ODS.PUBLIC.TRADE_ODS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_ACCOUNT"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_BROKER"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_COMPANY"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_CUSTOMER"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_DATE"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_FINANCIAL"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_INDUSTRY"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_SECURITY"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_STATUS_TYPE"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_TAX_RATE"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_TIME"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_TRADE"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_TRADE_TYPE"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.FACT_CASH_BALANCES"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.FACT_HOLDINGS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.FACT_MARKET_HISTORY"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.FACT_PROSPECT"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "TRUNCATE TABLE TPCDI_WH.PUBLIC.FACT_WATCHES"} ); rs = stmt.execute();
  // Reset all sequences
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE SEQUENCE TPCDI_WH.PUBLIC.DIM_ACCOUNT_SEQ START WITH=1 INCREMENT=1 COMMENT='DATABASE SEQUENCE TO SOURCE THE SURROGATE KEY FOR ACCOUNT.'"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE SEQUENCE TPCDI_WH.PUBLIC.DIM_BROKER_SEQ START WITH=1 INCREMENT=1 COMMENT='DATABASE SEQUENCE TO SOURCE THE SURROGATE KEY FOR BROKER.'"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE SEQUENCE TPCDI_WH.PUBLIC.DIM_COMPANY_SEQ START WITH=1 INCREMENT=1 COMMENT='DATABASE SEQUENCE TO SOURCE THE SURROGATE KEY FOR COMPANY.'"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE SEQUENCE TPCDI_WH.PUBLIC.DIM_CUSTOMER_SEQ START WITH=1 INCREMENT=1 COMMENT='DATABASE SEQUENCE TO SOURCE THE SURROGATE KEY FOR CUSTOMER.'"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE SEQUENCE TPCDI_WH.PUBLIC.DIM_SECURITY_SEQ START WITH=1 INCREMENT=1 COMMENT='DATABASE SEQUENCE TO SOURCE THE SURROGATE KEY FOR SECURITY.'"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE SEQUENCE TPCDI_WH.PUBLIC.CTRL_BATCH_SEQ START WITH=1 INCREMENT=1 COMMENT='DATABASE SEQUENCE TO SOURCE THE BATCH_ID FOR THE BATCH CONTROL TABLE.'"} ); rs = stmt.execute();
  // Reset all streams
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.ACCOUNT_STG_STM ON TABLE TPCDI_STG.PUBLIC.ACCOUNT_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.CASHTRANSACTION_STG_STM ON TABLE TPCDI_STG.PUBLIC.CASHTRANSACTION_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.CUSTOMER_STG_STM ON TABLE TPCDI_STG.PUBLIC.CUSTOMER_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.DAILYMARKET_STG_STM ON TABLE TPCDI_STG.PUBLIC.DAILYMARKET_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.DATE_STG_STM ON TABLE TPCDI_STG.PUBLIC.DATE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.HOLDINGHISTORY_STG_STM ON TABLE TPCDI_STG.PUBLIC.HOLDINGHISTORY_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.HR_STG_STM ON TABLE TPCDI_STG.PUBLIC.HR_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.INDUSTRY_STG_STM ON TABLE TPCDI_STG.PUBLIC.INDUSTRY_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.PROSPECT_STG_STM ON TABLE TPCDI_STG.PUBLIC.PROSPECT_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.STATUSTYPE_STG_STM ON TABLE TPCDI_STG.PUBLIC.STATUSTYPE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.TAXRATE_STG_STM ON TABLE TPCDI_STG.PUBLIC.TAXRATE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.TIME_STG_STM ON TABLE TPCDI_STG.PUBLIC.TIME_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.TRADEHISTORY_STG_I_STM ON TABLE TPCDI_STG.PUBLIC.TRADEHISTORY_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.TRADEHISTORY_STG_U_STM ON TABLE TPCDI_STG.PUBLIC.TRADEHISTORY_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.TRADETYPE_STG_STM ON TABLE TPCDI_STG.PUBLIC.TRADETYPE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.TRADE_STG_I_STM ON TABLE TPCDI_STG.PUBLIC.TRADE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.TRADE_STG_UC_STM ON TABLE TPCDI_STG.PUBLIC.TRADE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.TRADE_STG_US_STM ON TABLE TPCDI_STG.PUBLIC.TRADE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.TRADE_STG_U_STM ON TABLE TPCDI_STG.PUBLIC.TRADE_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.WATCH_HISTORY_STG_ACTV_STM ON TABLE TPCDI_STG.PUBLIC.WATCH_HISTORY_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.WATCH_HISTORY_STG_CNCL_STM ON TABLE TPCDI_STG.PUBLIC.WATCH_HISTORY_STG"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_ODS.PUBLIC.ACCOUNT_ODS_STM ON TABLE TPCDI_ODS.PUBLIC.ACCOUNT_ODS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_ODS.PUBLIC.CUSTOMER_ODS_STM ON TABLE TPCDI_ODS.PUBLIC.CUSTOMER_ODS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_ODS.PUBLIC.FINWIRE_CMP_ODS_STM ON TABLE TPCDI_ODS.PUBLIC.FINWIRE_CMP_ODS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_ODS.PUBLIC.FINWIRE_FIN_ODS_STM ON TABLE TPCDI_ODS.PUBLIC.FINWIRE_FIN_ODS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_ODS.PUBLIC.FINWIRE_SEC_ODS_STM ON TABLE TPCDI_ODS.PUBLIC.FINWIRE_SEC_ODS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_ODS.PUBLIC.TRADE_ODS_STM ON TABLE TPCDI_ODS.PUBLIC.TRADE_ODS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_WH.PUBLIC.DIM_ACCOUNT_STM ON TABLE TPCDI_ODS.PUBLIC.FINWIRE_SEC_ODS"} ); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "CREATE OR REPLACE STREAM TPCDI_WH.PUBLIC.DIM_SECURITY_STM ON TABLE TPCDI_ODS.PUBLIC.TRADE_ODS"} ); rs = stmt.execute();
  rs.next();
  return "All objects have been reset.";
  $$
;
