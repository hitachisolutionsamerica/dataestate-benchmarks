CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.DIM_SECURITY_HISTORICAL_SP()
  returns string
  language javascript
  as
  $$
  // Process SEC Records and Dimension Table
  var sec_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FINWIRE_SEC_ODS_SP(4)"}
      );
  sec_stmt.execute();
  return 'All historical Security records have been processed.';
  $$
;
