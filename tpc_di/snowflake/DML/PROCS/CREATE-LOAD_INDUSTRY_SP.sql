CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_INDUSTRY_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load INDUSTRY_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.INDUSTRY_STG FROM @TPCDI_FILES/tpcdi-" + tpcdi_scale + "/Batch1/Industry.txt FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE')"}
    );
  rs = stmt.execute();
  rs.next();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_INDUSTRY_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All industry files have been loaded.";
  $$
;
