CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.DIM_COMPANY_HISTORICAL_SP()
  returns string
  language javascript
  as
  $$
  // Process CMP Records and Dimension Table
  var ods_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FINWIRE_CMP_ODS_SP(4)"}
      );
  ods_stmt.execute();
  return 'All historical Company ODS records processed.';
  $$
;
