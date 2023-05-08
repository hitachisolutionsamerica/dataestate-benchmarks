CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.MAINTENANCE_SP()
  returns string
  language javascript
  as
  $$
  // Suspend Broker Task
  var dim_stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_BROKER_HISTORICAL_TSK SUSPEND"}
    );
  dim_stmt.execute();
  // Suspend Customer Task
  var dim_stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_CUSTOMER_HISTORICAL_TSK SUSPEND"}
    );
  dim_stmt.execute();
  // Suspend Company Task
  var dim_stmt = snowflake.createStatement(
      {sqlText: "ALTER TASK TPCDI_WH.PUBLIC.DIM_COMPANY_HISTORICAL_TSK SUSPEND"}
    );
  dim_stmt.execute();
  // Drop DimAccount Stream
  var dim_stmt = snowflake.createStatement(
      {sqlText: "DROP STREAM IF EXISTS TPCDI_WH.PUBLIC.DIM_ACCOUNT_STM"}
    );
  dim_stmt.execute();
  // Drop DimSecurity Stream
  var dim_stmt = snowflake.createStatement(
      {sqlText: "DROP STREAM IF EXISTS TPCDI_WH.PUBLIC.DIM_SECURITY_STM"}
    );
  dim_stmt.execute();
  return "Dummy executed and streams dropped.";
  $$
;
