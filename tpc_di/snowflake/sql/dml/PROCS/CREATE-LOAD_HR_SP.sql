CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_HR_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load HR_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.HR_STG FROM @TPCDI_FILES/tpcdi-" + tpcdi_scale + "/Batch1/HR FILE_FORMAT = (FORMAT_NAME = 'TXT_CSV')"}
    );
  rs = stmt.execute();
  rs.next();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_HR_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All HR files have been loaded.";
  $$
;
