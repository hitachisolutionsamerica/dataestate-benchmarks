CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.FACT_PROSPECT_MASTER_SP()
  returns string
  language javascript
  as
  $$
  // Process Transactions
  var fact_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_PROSPECT_TRANS_SP()"}
      );
  fact_stmt.execute();
  // Reset Marketing Nameplate
  var namep_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_PROSPECT_MN_RESET_SP()"}
      );
  namep_stmt.execute();
  // Marketing Nameplate HIGHVALUE
  var hv_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_PROSPECT_MN_HIGHVALUE_SP()"}
      );
  hv_stmt.execute();
  // Marketing Nameplate EXPENSES
  var exp_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_PROSPECT_MN_EXPENSES_SP()"}
      );
  exp_stmt.execute();
  // Marketing Nameplate BOOMER
  var boom_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_PROSPECT_MN_BOOMER_SP()"}
      );
  boom_stmt.execute();
  // Marketing Nameplate MONEYALERT
  var mony_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_PROSPECT_MN_MONEYALERT_SP()"}
      );
  mony_stmt.execute();
  // Marketing Nameplate SPENDER
  var spnd_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_PROSPECT_MN_SPENDER_SP()"}
      );
  spnd_stmt.execute();
  // Marketing Nameplate INHERITED
  var inher_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_PROSPECT_MN_INHERITED_SP()"}
      );
  inher_stmt.execute();
  // Update IS CUSTOMER
  var isc_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.FACT_PROSPECT_ISCUST_SP()"}
      );
  isc_stmt.execute();
  return 'All Prospect records processed.';
  $$
;  
