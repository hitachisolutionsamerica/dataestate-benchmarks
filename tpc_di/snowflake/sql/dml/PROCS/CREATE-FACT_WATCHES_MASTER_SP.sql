CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.FACT_WATCHES_MASTER_SP()
  returns string
  language javascript
  as
  $$
  // Process Active Transactions
  var act_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_WATCHES_ACTV_SP()"}
      );
  act_stmt.execute();
  // Process Cancelled Transactions
  var can_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_WATCHES_CNCL_SP()"}
      );
  can_stmt.execute();
  return 'Fact Watches load complete.';
  $$
;  
