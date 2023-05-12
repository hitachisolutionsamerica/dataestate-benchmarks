-- This procedure updates the ACCOUNT_ODS table with closed accounts from the historical load.
CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.ACCOUNT_INACT_ODS_SP()
  returns string
  language javascript
  as
  $$
  // Process ODS Table
  var stmt = snowflake.createStatement(
      {sqlText: "MERGE INTO TPCDI_ODS.PUBLIC.ACCOUNT_ODS USING (SELECT CUSTOMER_ID, CUSTOMER_ACCOUNT_ID FROM TPCDI_STG.PUBLIC.ACCOUNT_CLOSE_STG) ACS ON ACS.CUSTOMER_ACCOUNT_ID = ACCOUNT_ODS.CA_ID WHEN MATCHED THEN UPDATE SET ACCOUNT_ODS.CA_ST_ID = 'INAC', ACCOUNT_ODS.LAST_UPDATED_TS = CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT VALUES (ACS.CUSTOMER_ACCOUNT_ID, NULL, ACS.CUSTOMER_ID, NULL, NULL, 'INAC', CURRENT_TIMESTAMP())"}
    );
  stmt.execute();
  // Log audit record
  var audit_stmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_WH.PUBLIC.AUDIT SELECT 'ACCOUNT_INACT_ODS_SP', LOCALTIMESTAMP(), (SELECT MAX(BATCH_ID) FROM TPCDI_WH.PUBLIC.CTRL_BATCH), $1, $2 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"}
    );
  audit_stmt.execute();
  return 'Inactive Account ODS records processed.';
  $$
;