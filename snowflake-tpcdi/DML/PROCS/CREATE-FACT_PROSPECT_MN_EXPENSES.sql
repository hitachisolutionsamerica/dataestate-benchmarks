CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.FACT_PROSPECT_MN_EXPENSES_SP()
  returns string
  language javascript
  as
  $$
  // Process Fact Table
  var fact_stmt = snowflake.createStatement(
      {sqlText: "UPDATE TPCDI_WH.PUBLIC.FACT_PROSPECT SET MARKETING_NAMEPLATE = IFF(MARKETING_NAMEPLATE IS NULL,'EXPENSES',CONCAT(MARKETING_NAMEPLATE,'+EXPENSES')) WHERE NUMBER_CHILDREN > 3 OR NUMBER_CREDIT_CARDS > 5"}
    );
  fact_stmt.execute();
  // Log audit record
  var audit_stmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_WH.PUBLIC.AUDIT SELECT 'FACT_PROSPECT_MN_EXPENSES_SP', LOCALTIMESTAMP(), (SELECT MAX(BATCH_ID) FROM TPCDI_WH.PUBLIC.CTRL_BATCH), 0, $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"}
    );
  audit_stmt.execute();
  return 'Marketing Nameplate Expenses updated.';
  $$
;
