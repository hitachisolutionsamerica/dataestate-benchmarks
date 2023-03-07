CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.START_LOAD_INCREMENTAL_TASKS_SP(scale float)
  returns string
  language javascript
  as
  $$
  // Purpose: resume all tasks associated with loading historical files into the tpcdi_stg database
  // tpcdi_scale is an input variable that represents the TPC-DI data size; 5,10,100,1000 are possible values
  var tpcdi_scale = SCALE
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_CUSTOMER_I_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_ACCOUNT_I_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_I_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_DAILYMARKET_I_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_HOLDING_HISTORY_I_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TRADE_I_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_I_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_PROSPECT_I_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  return "All incremental load tasks started.";
  $$
;
