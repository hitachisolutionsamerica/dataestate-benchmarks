CREATE OR REPLACE TASK TPCDI_STG.PUBLIC.LOAD_INDUSTRY_1000_TSK
  WAREHOUSE = TPCDI_FILE_LOAD,
  SCHEDULE = '10 SECOND'
AS
CALL TPCDI_STG.PUBLIC.LOAD_INDUSTRY_SP(1000)
;