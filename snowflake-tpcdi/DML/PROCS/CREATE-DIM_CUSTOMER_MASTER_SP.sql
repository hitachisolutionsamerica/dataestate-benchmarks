CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.DIM_CUSTOMER_MASTER_SP()
  returns string
  language javascript
  as
  $$
  // Add a row to the batch table
  var btch_stmt = snowflake.createStatement({sqlText: "INSERT INTO TPCDI_WH.PUBLIC.CTRL_BATCH VALUES (TPCDI_WH.PUBLIC.CTRL_BATCH_SEQ.NEXTVAL,LOCALTIMESTAMP())"});
  btch_stmt.execute();
  // Process ODS Table
  var ods_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.CUSTOMER_ODS_SP()"}
      );
  ods_stmt.execute();
  // Process Dimension Table
  var dim_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_CUSTOMER_SP()"}
      );
  dim_stmt.execute();
  return 'All Customer Dimension records processed.';
  $$
;
