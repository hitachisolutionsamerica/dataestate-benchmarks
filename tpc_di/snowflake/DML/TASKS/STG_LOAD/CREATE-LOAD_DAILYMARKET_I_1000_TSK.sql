CREATE OR REPLACE TASK TPCDI_STG.PUBLIC.LOAD_DAILYMARKET_I_1000_TSK
  WAREHOUSE = TPCDI_FILE_LOAD,
  SCHEDULE = '10 SECOND'
AS
CALL TPCDI_STG.PUBLIC.LOAD_DAILYMARKET_I_SP(1000,3,60)
;
