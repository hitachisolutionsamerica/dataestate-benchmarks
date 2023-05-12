CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.FACT_MARKET_HISTORY_HISTORICAL_MASTER_SP()
  returns string
  language javascript
  as
  $$
  // Suspend Parent Task
  var tsk1_stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_SECURITY_HISTORICAL_TSK SUSPEND"}
      );
  tsk1_stmt.execute();
  // Process Transactions
  var fact_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_MARKET_HISTORY_TRANS_SP()"}
      );
  fact_stmt.execute();
  // Calculate Highs/Lows
  var hilo_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_MARKET_HISTORY_CALC_HIGH_LOW_SP()"}
      );
  hilo_stmt.execute();
  // Calculate PE Ratio
  var pe_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_MARKET_HISTORY_CALC_PE_RATIO_SP()"}
      );
  pe_stmt.execute();
  // Stop Task
  var tsk2_stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_MARKET_HISTORY_HISTORICAL_TSK SUSPEND"}
      );
  tsk2_stmt.execute();
  return 'All historical Market History fact records processed.';
  $$
;  
