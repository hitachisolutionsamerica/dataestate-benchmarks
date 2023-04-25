CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.FACT_WATCHES_INCREMENTAL_SP()
  returns string
  language javascript
  as
  $$
  // Call Master Procedure
  var mp_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_WATCHES_MASTER_SP()"}
      );
  mp_stmt.execute();
  return 'Incremental load complete.';
  $$
;  
