CREATE OR REPLACE TASK TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_I_100_TSK
  WAREHOUSE = TPCDI_FILE_LOAD,
  SCHEDULE = '10 SECOND'
AS
CALL TPCDI_STG.PUBLIC.LOAD_CASHTRANSACTION_I_SP(100,3,60)
;