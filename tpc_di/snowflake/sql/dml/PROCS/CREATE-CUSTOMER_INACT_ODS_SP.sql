CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.CUSTOMER_INACT_ODS_SP()
  returns string
  language javascript
  as
  $$
  // Process ODS Table
  var stmt = snowflake.createStatement(
      {sqlText: "UPDATE TPCDI_ODS.PUBLIC.CUSTOMER_ODS ODS SET ODS.C_ST_ID = 'INAC', ODS.LAST_UPDATED_TS = CURRENT_TIMESTAMP() FROM TPCDI_STG.PUBLIC.CUSTOMER_INACT_STG CIS WHERE CIS.C_ID = ODS.C_ID"}
    );
  stmt.execute();
  // Log audit record
  var audit_stmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_WH.PUBLIC.AUDIT SELECT 'CUSTOMER_INACT_ODS_SP', LOCALTIMESTAMP(), (SELECT MAX(BATCH_ID) FROM TPCDI_WH.PUBLIC.CTRL_BATCH), 0, $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"}
    );
  audit_stmt.execute();
  return 'Inactive customers processed.';
  $$
;
