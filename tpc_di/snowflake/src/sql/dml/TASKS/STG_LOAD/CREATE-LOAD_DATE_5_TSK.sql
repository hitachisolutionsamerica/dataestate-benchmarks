CREATE OR REPLACE TASK TPCDI_STG.PUBLIC.LOAD_DATE_5_TSK
  WAREHOUSE = TPCDI_FILE_LOAD
  
AS
CALL TPCDI_STG.PUBLIC.LOAD_DATE_SP(5)
;
