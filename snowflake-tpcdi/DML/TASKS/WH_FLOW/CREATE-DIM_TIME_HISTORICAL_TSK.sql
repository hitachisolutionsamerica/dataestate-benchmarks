CREATE OR REPLACE TASK TPCDI_WH.PUBLIC.DIM_TIME_HISTORICAL_TSK
  WAREHOUSE = TASK_WH_B,
  SCHEDULE = '5 SECOND'
WHEN
  SYSTEM$STREAM_HAS_DATA('TPCDI_STG.PUBLIC.TIME_STG_STM')
AS
CALL TPCDI_WH.PUBLIC.DIM_TIME_HISTORICAL_SP()
;
