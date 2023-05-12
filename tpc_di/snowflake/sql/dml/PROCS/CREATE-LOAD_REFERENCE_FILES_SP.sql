CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_REFERENCE_FILES_SP()
  returns string
  language javascript
  as
  $$
  // Load Reference Files
  // Load DATE_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_DATE_SP()"}
    );
  rs = stmt.execute();
  // Load TIME_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_TIME_SP()"}
    );
  rs = stmt.execute();
  // Load TRADETYPE_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_TRADETYPE_SP()"}
    );
  rs = stmt.execute();
  // Load STATUSTYPE_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_STATUSTYPE_SP()"}
    );
  rs = stmt.execute();
  // Load TAXRATE_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_TAXRATE_SP()"}
    );
  rs = stmt.execute();
  // Load INDUSTRY_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_INDUSTRY_SP()"}
    );
  rs = stmt.execute();
  // Load HR_STG
  stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_STG.PUBLIC.LOAD_HR_SP()"}
    );
  rs = stmt.execute();
  rs.next();
  return "All reference files have been loaded.";
  $$
;
