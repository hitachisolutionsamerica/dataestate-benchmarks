CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.FACT_MARKET_HISTORY_INCREMENTAL_MASTER_SP()
  returns string
  language javascript
  as
  $$
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
  return 'All incremental Market History records have been processed.';
  $$
;  
