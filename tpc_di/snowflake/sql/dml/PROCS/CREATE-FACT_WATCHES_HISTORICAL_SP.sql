CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.FACT_WATCHES_HISTORICAL_SP()
  returns string
  language javascript
  as
  $$
  // Call Master Procedure
  var mp_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_WATCHES_MASTER_SP()"}
      );
  mp_stmt.execute();
  // Stop Task
  var tsk_stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_WATCHES_HISTORICAL_TSK SUSPEND"}
      );
  tsk_stmt.execute();
  return 'Historical load complete.';
  $$
;  
