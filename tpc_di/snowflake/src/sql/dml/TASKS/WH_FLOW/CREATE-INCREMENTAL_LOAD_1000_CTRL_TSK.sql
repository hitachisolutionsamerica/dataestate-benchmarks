CREATE OR REPLACE TASK TPCDI_WH.PUBLIC.INCREMENTAL_LOAD_1000_CTRL_TSK
  WAREHOUSE = TASK_WH_B
  AFTER TPCDI_WH.PUBLIC.INCREMENTAL_DW_1000_CTRL_TSK
AS
CALL TPCDI_STG.PUBLIC.START_LOAD_INCREMENTAL_TASKS_SP(1000)
;