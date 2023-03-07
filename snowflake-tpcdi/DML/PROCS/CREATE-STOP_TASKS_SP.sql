CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.STOP_TASKS_SP()
  returns string
  language javascript
  as
  $$
  // Purpose: procedure to provide an easy way to stop all tasks
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.LOAD_SNAPSHOT_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_REFERENCE_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_DATE_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_TIME_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_INDUSTRY_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_TAX_RATE_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_STATUS_TYPE_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_TRADE_TYPE_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_CUSTOMER_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_BROKER_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_ACCOUNT_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_COMPANY_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_FINANCIAL_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_SECURITY_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_TRADE_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_PROSPECT_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_CASH_BALANCES_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_MARKET_HISTORY_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_HOLDINGS_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_WATCHES_HISTORICAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.CHECK_STREAM_CTRL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_CUSTOMER_INCREMENTAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_ACCOUNT_INCREMENTAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_TRADE_INCREMENTAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_PROSPECT_INCREMENTAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_CASH_BALANCES_INCREMENTAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_MARKET_HISTORY_INCREMENTAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_HOLDINGS_INCREMENTAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_WATCHES_INCREMENTAL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.INCREMENTAL_LOAD_5_CTRL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.INCREMENTAL_LOAD_10_CTRL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.INCREMENTAL_LOAD_100_CTRL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.INCREMENTAL_LOAD_1000_CTRL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.INCREMENTAL_DW_5_CTRL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.INCREMENTAL_DW_10_CTRL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.INCREMENTAL_DW_100_CTRL_TSK SUSPEND"}); rs = stmt.execute();
  stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.INCREMENTAL_DW_1000_CTRL_TSK SUSPEND"}); rs = stmt.execute();
  rs.next();
  output = rs.getColumnValue(1);
  return output;
  $$
;
