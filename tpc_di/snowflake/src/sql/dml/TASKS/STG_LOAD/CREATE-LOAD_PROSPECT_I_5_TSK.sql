CREATE OR REPLACE TASK TPCDI_STG.PUBLIC.LOAD_PROSPECT_I_5_TSK
  WAREHOUSE = TPCDI_FILE_LOAD
  
AS
CALL TPCDI_STG.PUBLIC.LOAD_PROSPECT_I_SP(5)
;
