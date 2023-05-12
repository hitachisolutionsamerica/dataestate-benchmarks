CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.FINWIRE_CMP_ODS_SP(levels float)
  returns string
  language javascript
  as
  $$
  var level_counter = 1
  while (level_counter <= LEVELS)
  {
  // Process ODS Table
  var ods_stmt = snowflake.createStatement(
      {sqlText: "MERGE INTO TPCDI_ODS.PUBLIC.FINWIRE_CMP_ODS USING (WITH NTH_PTS_DATE_ROW AS (SELECT DISTINCT NTH_VALUE(C.PTS," + level_counter + ") OVER (PARTITION BY C.CIK ORDER BY C.PTS) AS NTH_PTS_DATE, C.CIK AS C_CIK FROM TPCDI_STG.PUBLIC.FINWIRE_CMP_STG C) SELECT FCS.PTS, FCS.REC_TYPE, FCS.COMPANY_NAME, FCS.CIK, FCS.STATUS, FCS.INDUSTRY_ID, FCS.SP_RATING, FCS.FOUNDING_DATE, FCS.ADDR_LINE1, FCS.ADDR_LINE2, FCS.POSTAL_CODE, FCS.CITY, FCS.STATE_PROVINCE, FCS.COUNTRY, FCS.CEO_NAME, FCS.DESCRIPTION FROM TPCDI_STG.PUBLIC.FINWIRE_CMP_STG FCS JOIN NTH_PTS_DATE_ROW ON NTH_PTS_DATE_ROW.NTH_PTS_DATE = FCS.PTS AND NTH_PTS_DATE_ROW.C_CIK = FCS.CIK) FINWIRE_CMP_STG ON TPCDI_ODS.PUBLIC.FINWIRE_CMP_ODS.CIK = FINWIRE_CMP_STG.CIK WHEN MATCHED THEN UPDATE SET FINWIRE_CMP_ODS.PTS = FINWIRE_CMP_STG.PTS, FINWIRE_CMP_ODS.REC_TYPE = FINWIRE_CMP_STG.REC_TYPE, FINWIRE_CMP_ODS.COMPANY_NAME = FINWIRE_CMP_STG.COMPANY_NAME, FINWIRE_CMP_ODS.STATUS = FINWIRE_CMP_STG.STATUS, FINWIRE_CMP_ODS.INDUSTRY_ID = FINWIRE_CMP_STG.INDUSTRY_ID, FINWIRE_CMP_ODS.SP_RATING = FINWIRE_CMP_STG.SP_RATING, FINWIRE_CMP_ODS.FOUNDING_DATE = FINWIRE_CMP_STG.FOUNDING_DATE, FINWIRE_CMP_ODS.ADDR_LINE1 = FINWIRE_CMP_STG.ADDR_LINE1, FINWIRE_CMP_ODS.ADDR_LINE2 = FINWIRE_CMP_STG.ADDR_LINE2, FINWIRE_CMP_ODS.POSTAL_CODE = FINWIRE_CMP_STG.POSTAL_CODE, FINWIRE_CMP_ODS.CITY = FINWIRE_CMP_STG.CITY, FINWIRE_CMP_ODS.STATE_PROVINCE = FINWIRE_CMP_STG.STATE_PROVINCE, FINWIRE_CMP_ODS.COUNTRY = FINWIRE_CMP_STG.COUNTRY, FINWIRE_CMP_ODS.CEO_NAME = FINWIRE_CMP_STG.CEO_NAME, FINWIRE_CMP_ODS.DESCRIPTION = FINWIRE_CMP_STG.DESCRIPTION, FINWIRE_CMP_ODS.LAST_UPDATED_TS = CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT VALUES ( FINWIRE_CMP_STG.PTS, FINWIRE_CMP_STG.REC_TYPE, FINWIRE_CMP_STG.COMPANY_NAME, FINWIRE_CMP_STG.CIK, FINWIRE_CMP_STG.STATUS, FINWIRE_CMP_STG.INDUSTRY_ID, FINWIRE_CMP_STG.SP_RATING, FINWIRE_CMP_STG.FOUNDING_DATE, FINWIRE_CMP_STG.ADDR_LINE1, FINWIRE_CMP_STG.ADDR_LINE2, FINWIRE_CMP_STG.POSTAL_CODE, FINWIRE_CMP_STG.CITY, FINWIRE_CMP_STG.STATE_PROVINCE, FINWIRE_CMP_STG.COUNTRY, FINWIRE_CMP_STG.CEO_NAME, FINWIRE_CMP_STG.DESCRIPTION, CURRENT_TIMESTAMP())"}
    );
  ods_stmt.execute();
  // Log audit record
  var audit_stmt = snowflake.createStatement(
      {sqlText: "INSERT INTO TPCDI_WH.PUBLIC.AUDIT SELECT 'FINWIRE_CMP_ODS_SP', LOCALTIMESTAMP(), (SELECT MAX(BATCH_ID) FROM TPCDI_WH.PUBLIC.CTRL_BATCH), $1, $2 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"}
    );
  audit_stmt.execute();
  // Process Dimension Table Updates
  var dim_stmt = snowflake.createStatement(
      {sqlText: "CALL TPCDI_WH.PUBLIC.DIM_COMPANY_SP()"}
    );
  dim_stmt.execute();
  level_counter++
  }
  return 'Company records processed.';
  $$
;