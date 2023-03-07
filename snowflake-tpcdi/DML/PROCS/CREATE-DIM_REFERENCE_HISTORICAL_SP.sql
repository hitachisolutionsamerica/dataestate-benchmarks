CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.DIM_REFERENCE_HISTORICAL_SP()
  returns string
  language javascript
  as
  $$
  // Run DIM_DATE_HISTORICAL Procedure
  var date_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_DATE_HISTORICAL_SP()"}
    );
  date_stmt.execute();
  // Run DIM_TIME_HISTORICAL Procedure
  var time_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_TIME_HISTORICAL_SP()"}
    );
  time_stmt.execute();
  // Run DIM_TRADE_TYPE_HISTORICAL Procedure
  var tt_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_TRADE_TYPE_HISTORICAL_SP()"}
    );
  tt_stmt.execute();
  // Run DIM_STATUS_TYPE_HISTORICAL Procedure
  var st_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_STATUS_TYPE_HISTORICAL_SP()"}
    );
  st_stmt.execute();
  // Run DIM_TAX_RATE_HISTORICAL Procedure
  var tr_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_TAX_RATE_HISTORICAL_SP()"}
    );
  tr_stmt.execute();
  // Run DIM_INDUSTRY_HISTORICAL Procedure
  var ind_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_INDUSTRY_HISTORICAL_SP()"}
    );
  ind_stmt.execute();
  return "All reference dimension tasks have been started.";
  $$
;
