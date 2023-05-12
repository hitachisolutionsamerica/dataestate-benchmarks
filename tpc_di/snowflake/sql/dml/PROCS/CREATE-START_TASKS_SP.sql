CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.START_TASKS_SP()
  returns string
  language javascript
  as
  $$
  // Purpose: this procedure is now defunct
  // Start LOAD_SNAPSHOT Task
  stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.LOAD_SNAPSHOT_TSK RESUME"}
    );
  rs = stmt.execute();
  // Wait 10 Seconds
  stmt = snowflake.createStatement({sqlText:`call system$wait(10, 'SECONDS');`});
  rs = stmt.execute();
  // Start DIM_DATE_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_DATE_HISTORICAL_TSK RESUME"}
    );
  rs = stmt.execute();
 // Start DIM_TIME_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_TIME_HISTORICAL_TSK RESUME"}
    );
  rs = stmt.execute();
  // Start DIM_TRADE_TYPE_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_TRADE_TYPE_HISTORICAL_TSK RESUME"}
    );
  rs = stmt.execute();
  // Start DIM_STATUS_TYPE_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_STATUS_TYPE_HISTORICAL_TSK RESUME"}
    );
  rs = stmt.execute();
  // Start DIM_TAX_RATE_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_TAX_RATE_HISTORICAL_TSK RESUME"}
    );
  rs = stmt.execute();
  // Start DIM_INDUSTRY_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_INDUSTRY_HISTORICAL_TSK RESUME"}
    );
  rs = stmt.execute();
  // Start DIM_BROKER_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_BROKER_HISTORICAL_TSK RESUME"}
    );
  rs = stmt.execute();
  // Wait 30 Seconds
  stmt = snowflake.createStatement({sqlText:`call system$wait(30, 'SECONDS');`});
  rs = stmt.execute();
  // Start DIM_CUSTOMER_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_CUSTOMER_HISTORICAL_TSK RESUME"}
    );
  rs = stmt.execute();
  // Wait 30 Seconds
  stmt = snowflake.createStatement({sqlText:`call system$wait(30, 'SECONDS');`});
  rs = stmt.execute();
  // Start DIM_ACCOUNT_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_ACCOUNT_HISTORICAL_TSK RESUME"}
    );
  rs = stmt.execute();
  // Wait 60 Seconds
  stmt = snowflake.createStatement({sqlText:`call system$wait(60, 'SECONDS');`});
  rs = stmt.execute();
  // Start DIM_CUSTOMER Task
  stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_CUSTOMER_TSK RESUME"}
    );
  rs = stmt.execute();
  // Wait 10 Seconds
  stmt = snowflake.createStatement({sqlText:`call system$wait(10, 'SECONDS');`});
  rs = stmt.execute();
  // Start DIM_ACCOUNT Task
  stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_ACCOUNT_TSK RESUME"}
    );
  rs = stmt.execute();
  // Wait 10 Seconds
  stmt = snowflake.createStatement({sqlText:`call system$wait(10, 'SECONDS');`});
  rs = stmt.execute();
  // Start FACT_PROSPECT Task
  stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.FACT_PROSPECT_TSK RESUME"}
    );
  rs = stmt.execute();
  rs.next();
  output = rs.getColumnValue(1);
  return output;
  $$
;
