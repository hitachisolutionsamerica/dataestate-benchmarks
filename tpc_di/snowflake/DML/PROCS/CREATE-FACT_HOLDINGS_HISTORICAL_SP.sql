CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.FACT_HOLDINGS_HISTORICAL_SP()
  returns string
  language javascript
  as
  $$
  // Suspend Parent Task
  var tsk_stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_TRADE_HISTORICAL_TSK SUSPEND"}
      );
  tsk_stmt.execute();
  // Process Transactions
  var fact_stmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_WH.PUBLIC.FACT_HOLDINGS SELECT HH_H_T_ID, HH_T_ID, DIM_TRADE.SK_CUSTOMER_ID, DIM_TRADE.SK_ACCOUNT_ID, DIM_TRADE.SK_SECURITY_ID, DIM_TRADE.SK_COMPANY_ID, DIM_TRADE.SK_CLOSE_DATE_ID, DIM_TRADE.SK_CLOSE_TIME_ID, DIM_TRADE.TRADE_PRICE, HH_AFTER_QTY, (SELECT MAX(BATCH_ID) FROM TPCDI_WH.PUBLIC.CTRL_BATCH)  FROM TPCDI_STG.PUBLIC.HOLDINGHISTORY_STG_STM INNER JOIN TPCDI_WH.PUBLIC.DIM_TRADE ON HOLDINGHISTORY_STG_STM.HH_T_ID = DIM_TRADE.TRADE_ID"}
      );
  fact_stmt.execute();
  // Log audit record
  var audit_stmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_WH.PUBLIC.AUDIT SELECT 'FACT_HOLDINGS_HISTORICAL_SP', LOCALTIMESTAMP(), (SELECT MAX(BATCH_ID) FROM TPCDI_WH.PUBLIC.CTRL_BATCH), $1, 0 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"}
      );
  audit_stmt.execute();
  // Stop Task
  var tsk_stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.CHECK_STREAM_CTRL_TSK SUSPEND"}
      );
  tsk_stmt.execute();
  return 'Fact Holdings historical records processed.';
  $$
;  
