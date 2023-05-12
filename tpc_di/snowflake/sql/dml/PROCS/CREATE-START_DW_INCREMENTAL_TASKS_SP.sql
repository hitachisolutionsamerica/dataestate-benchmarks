CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.START_DW_INCREMENTAL_TASKS_SP()
  returns string
  language javascript
  as
  $$
  // Purpose: resume all tasks associated with processing the incremental data into the tpcdi_wh database
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_CUSTOMER_INCREMENTAL_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_ACCOUNT_INCREMENTAL_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_TRADE_INCREMENTAL_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_CASH_BALANCES_INCREMENTAL_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_MARKET_HISTORY_INCREMENTAL_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_PROSPECT_INCREMENTAL_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_HOLDINGS_INCREMENTAL_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_WATCHES_INCREMENTAL_TSK RESUME"});
  stmt.execute();
  return "All incremental DW tasks started.";
  $$
;
