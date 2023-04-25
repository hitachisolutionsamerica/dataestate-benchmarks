CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_TAXRATE_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load TAXRATE_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.TAXRATE_STG FROM @TPCDI_FILES/tpcdi-" + tpcdi_scale + "/Batch1/TaxRate.txt FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE')"}
    );
  rs = stmt.execute();
  rs.next();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TAXRATE_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All taxrate files have been loaded.";
  $$
;
