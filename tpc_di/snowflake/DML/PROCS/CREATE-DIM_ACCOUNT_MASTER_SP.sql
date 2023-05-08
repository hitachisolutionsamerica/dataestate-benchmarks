CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.DIM_ACCOUNT_MASTER_SP()
  returns string
  language javascript
  as
  $$
  // Process ODS Table
  var ods_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.ACCOUNT_ODS_SP()"}
      );
  ods_stmt.execute();
  // Process Dimension Table
  var dim_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_ACCOUNT_SP()"}
      );
  dim_stmt.execute();
  return 'All Account Dimension records processed.';
  $$
;
