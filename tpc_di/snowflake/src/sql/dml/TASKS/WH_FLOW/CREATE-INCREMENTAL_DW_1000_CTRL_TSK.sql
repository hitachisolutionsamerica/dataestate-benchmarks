CREATE OR REPLACE TASK TPCDI_WH.PUBLIC.INCREMENTAL_DW_1000_CTRL_TSK
  WAREHOUSE = TASK_WH_B
  AFTER TPCDI_WH.PUBLIC.FACT_HOLDINGS_HISTORICAL_TSK
AS
CALL TPCDI_WH.PUBLIC.START_DW_INCREMENTAL_TASKS_SP()
;