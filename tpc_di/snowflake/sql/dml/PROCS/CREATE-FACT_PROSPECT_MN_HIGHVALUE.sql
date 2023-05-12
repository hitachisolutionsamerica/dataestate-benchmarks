CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.FACT_PROSPECT_MN_HIGHVALUE_SP()
  returns string
  language javascript
  as
  $$
  // Process Fact Table
  var fact_stmt = snowflake.createStatement(
      {sqlText: "UPDATE TPCDI_WH.PUBLIC.FACT_PROSPECT SET MARKETING_NAMEPLATE = 'HIGHVALUE' WHERE NET_WORTH > 1000000 OR INCOME > 200000"}
    );
  fact_stmt.execute();
  // Log audit record
  var audit_stmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_WH.PUBLIC.AUDIT SELECT 'FACT_PROSPECT_MN_HIGHVALUE_SP', LOCALTIMESTAMP(), (SELECT MAX(BATCH_ID) FROM TPCDI_WH.PUBLIC.CTRL_BATCH), 0, $1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"}
    );
  audit_stmt.execute();
  return 'Marketing Nameplate High Value updated.';
  $$
;
