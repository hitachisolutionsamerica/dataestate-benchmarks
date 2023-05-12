CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_PROSPECT_SP(scale float,files float,position float,wait float)
  returns string
  language javascript
  as
  $$
  // Load historical and incremental files	
  var tpcdi_scale = SCALE
  var file_counter = POSITION;
  while (file_counter <= FILES)
  {
    var incrm_stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.PROSPECT_STG FROM @TPCDI_FILES/tpcdi-" + tpcdi_scale + "/Batch" + file_counter + "/Prospect FILE_FORMAT = (FORMAT_NAME = 'TXT_CSV')"}
      );
    incrm_stmt.execute();
    // insert wait here
    stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
    rs = stmt.execute();	
    file_counter++
  }
  return "All prospect files have been loaded.";
  $$
;
