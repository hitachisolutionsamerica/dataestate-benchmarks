CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.DIM_CUSTOMER_HISTORICAL_SP()
  returns string
  language javascript
  as
  $$
  // Process NEW Records
  var nods_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.CUSTOMER_NEW_ODS_SP()"}
      );
  nods_stmt.execute();
  // Process Dimension Table
  var dim1_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_CUSTOMER_SP()"}
      );
  dim1_stmt.execute();
  // Process UPDT Records
  var uods_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.CUSTOMER_UPDT_ODS_SP()"}
      );
  uods_stmt.execute();
  // Process Dimension Table
  var dim2_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_CUSTOMER_SP()"}
      );
  dim2_stmt.execute();
  // Process INACT Records
  var iods_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.CUSTOMER_INACT_ODS_SP()"}
      );
  iods_stmt.execute();
  // Process Dimension Table
  var dim3_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_CUSTOMER_SP()"}
      );
  dim3_stmt.execute();
  return 'Dim Customer Historical completed.';
  $$
;
