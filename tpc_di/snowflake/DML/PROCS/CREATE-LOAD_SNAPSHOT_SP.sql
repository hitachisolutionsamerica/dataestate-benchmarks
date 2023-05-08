CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.LOAD_SNAPSHOT_SP()
  returns string
  language javascript
  as
  $$
  // Capture WH Tables
  var whstmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_WH.PUBLIC.LOAD_SNAPSHOT SELECT TABLE_CATALOG, LOCALTIMESTAMP(), SUM(ROW_COUNT) FROM TPCDI_WH.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME != 'LOAD_SNAPSHOT' and TABLE_NAME != 'CTRL_BATCH' AND TABLE_TYPE = 'BASE TABLE' GROUP BY 1,2"}
      );
  whstmt.execute();
  return "Snapshot successful.";
  $$
;
