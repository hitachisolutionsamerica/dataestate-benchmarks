-- This procedure inserts new accounts into ACCOUNT_ODS from the historical load.
CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.ACCOUNT_NEW_ODS_SP()
  returns string
  language javascript
  as
  $$
  // Process ODS Table
  var stmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_ODS.PUBLIC.ACCOUNT_ODS SELECT CUSTOMER_ACCOUNT_ID, CUSTOMER_ACCOUNT_BROKER_ID, CUSTOMER_ID, CUSTOMER_ACCOUNT_NAME, CUSTOMER_ACCOUNT_TAX_STATUS, 'ACTV' AS CA_ST_ID, CURRENT_TIMESTAMP() FROM TPCDI_STG.PUBLIC.ACCOUNT_NEW_STG"}
    );
  stmt.execute();
  // Log audit record
  var audit_stmt = snowflake.createStatement(
       {sqlText: "INSERT INTO TPCDI_WH.PUBLIC.AUDIT SELECT 'ACCOUNT_NEW_ODS_SP', LOCALTIMESTAMP(), (SELECT MAX(BATCH_ID) FROM TPCDI_WH.PUBLIC.CTRL_BATCH), $1, 0 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"}
    );
  audit_stmt.execute();
  return 'New Account ODS records processed.';
  $$
;
