CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_STATUSTYPE_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load STATUSTYPE_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.STATUSTYPE_STG FROM @TPCDI_FILES/tpcdi-" + tpcdi_scale + "/Batch1/StatusType.txt FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE')"}
    );
  rs = stmt.execute();
  rs.next();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_STATUSTYPE_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All statustype files have been loaded.";
  $$
;
