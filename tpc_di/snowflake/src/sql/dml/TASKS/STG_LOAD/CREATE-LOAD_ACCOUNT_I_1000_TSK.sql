CREATE OR REPLACE TASK TPCDI_STG.PUBLIC.LOAD_ACCOUNT_I_1000_TSK
  WAREHOUSE = TPCDI_FILE_LOAD
  
AS
CALL TPCDI_STG.PUBLIC.LOAD_ACCOUNT_SP(1000,3,60)
;