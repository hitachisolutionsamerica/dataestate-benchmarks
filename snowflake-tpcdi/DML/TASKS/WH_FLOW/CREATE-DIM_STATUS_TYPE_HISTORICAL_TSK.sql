CREATE OR REPLACE TASK TPCDI_WH.PUBLIC.DIM_STATUS_TYPE_HISTORICAL_TSK
  WAREHOUSE = TASK_WH_B,
  SCHEDULE = '5 SECOND'
WHEN
  SYSTEM$STREAM_HAS_DATA('TPCDI_STG.PUBLIC.STATUSTYPE_STG_STM')
AS
CALL TPCDI_WH.PUBLIC.DIM_STATUS_TYPE_HISTORICAL_SP()
;
