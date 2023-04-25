CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_CUSTOMER_MGMT_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load CUSTOMER_MGMT_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.CUSTOMER_MGMT_STG FROM @TPCDI_FILES/tpcdi-" + tpcdi_scale + "/Batch1/CustomerMgmt FILE_FORMAT = (FORMAT_NAME = 'XML') ON_ERROR=CONTINUE"}
    );
  rs = stmt.execute();
  rs.next();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_CUSTOMER_MGMT_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All customer_mgmt files have been loaded.";
  $$
;
