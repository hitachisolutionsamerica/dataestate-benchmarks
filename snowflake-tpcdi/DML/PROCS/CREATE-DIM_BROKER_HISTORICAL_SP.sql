CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.DIM_BROKER_HISTORICAL_SP()
  returns string
  language javascript
  as
  $$
  // Process Dimension Table Updates
  var dim_stmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_WH.PUBLIC.DIM_BROKER SELECT TPCDI_WH.PUBLIC.DIM_BROKER_SEQ.NEXTVAL, EMPLOYEEID, MANAGERID, EMPLOYEEFIRSTNAME, EMPLOYEELASTNAME, EMPLOYEEMI, EMPLOYEEBRANCH, EMPLOYEEOFFICE, EMPLOYEEPHONE, (SELECT MAX(BATCH_ID) FROM TPCDI_WH.PUBLIC.CTRL_BATCH), LOCALTIMESTAMP() FROM TPCDI_STG.PUBLIC.HR_STG_STM WHERE EMPLOYEEJOBCODE = 314 AND METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'FALSE'"}
    );
  dim_stmt.execute();
  // Log audit record
  var audit_stmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_WH.PUBLIC.AUDIT SELECT 'DIM_BROKER_HISTORICAL_SP', LOCALTIMESTAMP(), (SELECT MAX(BATCH_ID) FROM TPCDI_WH.PUBLIC.CTRL_BATCH), $1, 0 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"}
    );
  audit_stmt.execute();
  // Suspend task to load reference tables
  var tsk_stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_REFERENCE_HISTORICAL_TSK SUSPEND"}
    );
  tsk_stmt.execute();
  return "Broker records processed.";
  $$
;
