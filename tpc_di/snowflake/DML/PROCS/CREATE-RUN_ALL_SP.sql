CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.RUN_ALL_SP(scale float)
  returns string
  language javascript
  as
  $$
  // Purpose: from a completed run state, reset all objects, run the historical/incremental file loads and transformation tasks
  // tpcdi_scale is an input variable that represents the TPC-DI data size; 5,10,100,1000 are possible values
  var tpcdi_scale = SCALE
  // Call the proc that truncates/resets all objects in the demo
  var stmt = snowflake.createStatement({sqlText: "CALL TPCDI_WH.PUBLIC.RESET_ALL_SP()"});
  stmt.execute();
  // Write batch 1 into the ctrl_batch table
  var stmt = snowflake.createStatement({sqlText: "INSERT INTO TPCDI_WH.PUBLIC.CTRL_BATCH VALUES (TPCDI_WH.PUBLIC.CTRL_BATCH_SEQ.NEXTVAL,LOCALTIMESTAMP())"});
  stmt.execute();
  // Start the task that captures the total table row counts in the tpcdi_wh database every 10 seconds
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.LOAD_SNAPSHOT_TSK RESUME"});
  stmt.execute();
  // Start the task that loads historical files into the tpcdi_stg database
  var stmt = snowflake.createStatement({sqlText: "CALL TPCDI_STG.PUBLIC.START_LOAD_HISTORICAL_TASKS_SP(" + tpcdi_scale + ")"});
  stmt.execute();
  // Start the tasks that will run the historical load as historical files are loaded into tpcdi_stg
  var stmt = snowflake.createStatement({sqlText: "CALL TPCDI_WH.PUBLIC.START_DW_HISTORICAL_TASKS_SP()"});
  stmt.execute();
  // Start the tasks that loads incremental files into the tpcdi_stg database after the historical load finishes
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.INCREMENTAL_LOAD_" + tpcdi_scale + "_CTRL_TSK RESUME"});
  stmt.execute();
  // Start the tasks that will run the incremental load as incremental files are loaded into tpcdi_stg
  var stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_WH.PUBLIC.INCREMENTAL_DW_" + tpcdi_scale + "_CTRL_TSK RESUME"});
  stmt.execute();
  return "All tables have been reset, all historical tasks are started, and incremental flow control is ready.";
  $$
;
