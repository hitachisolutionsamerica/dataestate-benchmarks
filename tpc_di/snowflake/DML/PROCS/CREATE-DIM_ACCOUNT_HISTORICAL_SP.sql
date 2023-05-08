CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.DIM_ACCOUNT_HISTORICAL_SP()
  returns string
  language javascript
  as
  $$
  // Process NEW Records
  var nods_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.ACCOUNT_NEW_ODS_SP()"}
      );
  nods_stmt.execute();
  // Process Dimension Table
  var dim1_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_ACCOUNT_SP()"}
      );
  dim1_stmt.execute();
  // Process UPDT Records and Dimension Table
  var uods_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.ACCOUNT_UPDT_ODS_SP(7)"}
      );
  uods_stmt.execute();
  // Process INACT Records
  var iods_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.ACCOUNT_INACT_ODS_SP()"}
      );
  iods_stmt.execute();
  // Create Stream on Dimension for flow control
  var flow_stmt = snowflake.createStatement(
      {sqlText: "CREATE OR REPLACE STREAM TPCDI_WH.PUBLIC.DIM_ACCOUNT_STM ON TABLE TPCDI_WH.PUBLIC.DIM_ACCOUNT"}
      );
  flow_stmt.execute();
  // Process Dimension Table
  var dim2_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_ACCOUNT_SP()"}
      );
  dim2_stmt.execute();
  return 'All historical Account Dimension records processed.';
  $$
;
