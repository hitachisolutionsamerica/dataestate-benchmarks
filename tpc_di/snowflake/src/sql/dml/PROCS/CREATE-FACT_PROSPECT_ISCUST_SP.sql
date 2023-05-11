CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.FACT_PROSPECT_ISCUST_SP()
  returns string
  language javascript
  as
  $$
  // Process Fact Table
  var fact_stmt = snowflake.createStatement(
      {sqlText: "UPDATE TPCDI_WH.PUBLIC.FACT_PROSPECT T1 SET T1.IS_CUSTOMER = 'TRUE' FROM TPCDI_WH.PUBLIC.DIM_CUSTOMER_NOW T2 WHERE UPPER(T1.LAST_NAME) = UPPER(T2.LAST_NAME) AND UPPER(T1.FIRST_NAME) = UPPER(T2.FIRST_NAME) AND UPPER(T1.ADDRESS_LINE1) = UPPER(T2.ADDRESS_LINE_1) AND UPPER(T1.ADDRESS_LINE2) = UPPER(T2.ADDRESS_LINE_2) AND UPPER(T1.POSTAL_CODE) = UPPER(T2.POSTAL_CODE) AND UPPER(T2.STATUS) = 'ACTV'"}
    );
  fact_stmt.execute();
  // Log audit record
  var audit_stmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_WH.PUBLIC.AUDIT SELECT 'FACT_PROSPECT_ISCUST_SP', LOCALTIMESTAMP(), (SELECT MAX(BATCH_ID) FROM TPCDI_WH.PUBLIC.CTRL_BATCH), 0, $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"}
    );
  audit_stmt.execute();
  return 'Is Customer updated.';
  $$
;
