CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.START_LOAD_HISTORICAL_TASKS_SP(scale float)
  returns string
  language javascript
  as
  $$
  // Purpose: resume all tasks associated with loading historical files into the tpcdi_stg database
  // tpcdi_scale is an input variable that represents the TPC-DI data size; 5,10,100,1000 are possible values
  var tpcdi_scale = SCALE
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_DATE_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TIME_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TRADETYPE_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_STATUSTYPE_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TAXRATE_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_INDUSTRY_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_HR_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_CUSTOMER_MGMT_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_FINWIRE_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText:"call system$wait(10, 'SECONDS')"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_H_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_DAILYMARKET_H_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_HOLDING_HISTORY_H_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TRADE_H_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_H_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_PROSPECT_H_" + tpcdi_scale + "_TSK RESUME"});
  stmt.execute();
  return "All historical load tasks started.";
  $$
;
