CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.FINWIRE_FIN_ODS_SP()
  returns string
  language javascript
  as
  $$
  // Process ODS Table
  var stmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_ODS.PUBLIC.FINWIRE_FIN_ODS SELECT PTS, REC_TYPE, YEAR, QUARTER, QTR_START_DATE, POSTING_DATE, REVENUE, EARNINGS, EPS, DILUTED_EPS, MARGIN, INVENTORY, ASSETS, LIABILITIES, SH_OUT, DILUTED_SH_OUT, CO_NAME_OR_CIK, CURRENT_TIMESTAMP() FROM TPCDI_STG.PUBLIC.FINWIRE_FIN_STG"}
      );
  stmt.execute();
  // Log audit record
  var audit_stmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_WH.PUBLIC.AUDIT SELECT 'FINWIRE_FIN_ODS_SP', LOCALTIMESTAMP(), (SELECT MAX(BATCH_ID) FROM TPCDI_WH.PUBLIC.CTRL_BATCH), $1, 0 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"}
      );
  audit_stmt.execute();
  return 'Financial ODS records processed.';
  $$
;

