CREATE OR REPLACE TASK TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_H_1000_TSK
  WAREHOUSE = TPCDI_FILE_LOAD,
  SCHEDULE = '10 SECOND'
AS
CALL TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_H_SP(1000)
;
