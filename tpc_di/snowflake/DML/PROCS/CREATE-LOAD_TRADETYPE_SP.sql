CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_TRADETYPE_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load TRADETYPE_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.TRADETYPE_STG FROM @TPCDI_FILES/tpcdi-" + tpcdi_scale + "/Batch1/TradeType.txt FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE')"}
    );
  rs = stmt.execute();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TRADETYPE_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All tradetype files have been loaded.";
  $$
;
