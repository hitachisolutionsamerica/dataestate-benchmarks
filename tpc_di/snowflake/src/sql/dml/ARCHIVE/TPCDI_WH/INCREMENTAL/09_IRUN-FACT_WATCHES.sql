-- INCREMENTAL LOAD

-- ALTER SESSION TO SET PARAMETER TO ALLOW MULTIPLE INSTANCES OF SAME BUSINESS ID FOR UPDATES IN STAGE
ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = FALSE;

-- MERGE RECORDS FROM STAGE
MERGE INTO TPCDI_WH.PUBLIC.FACT_WATCHES USING
    (
  SELECT
       DIM_CUSTOMER.SK_CUSTOMER_ID SK_CUSTOMER_ID
     , DIM_SECURITY.SK_SECURITY_ID SK_SECURITY_ID
     , (CASE WHEN WATCH_HISTORY_STG.W_ACTION = 'ACTV' THEN DIM_DATE.DATE_ID END) SK_DATE_ID_DATE_PLACED
     , (CASE WHEN WATCH_HISTORY_STG.W_ACTION = 'CNCL' THEN DIM_DATE.DATE_ID END) SK_DATE_ID_DATE_REMOVED
     , TO_NUMBER(1) BATCH_ID
     , WATCH_HISTORY_STG.W_ACTION ACTION
  FROM TPCDI_STG.PUBLIC.WATCH_HISTORY_STG
  INNER JOIN TPCDI_WH.PUBLIC.DIM_CUSTOMER ON
      WATCH_HISTORY_STG.W_C_ID = DIM_CUSTOMER.CUSTOMER_ID
  INNER JOIN TPCDI_WH.PUBLIC.DIM_SECURITY ON
      WATCH_HISTORY_STG.W_S_SYMB = DIM_SECURITY.SYMBOL
  INNER JOIN TPCDI_WH.PUBLIC.DIM_DATE ON
      TO_DATE(WATCH_HISTORY_STG.W_DTS) = TO_DATE(DIM_DATE.DATE_VALUE)
    ) WATCHES_UPDATES ON FACT_WATCHES.SK_CUSTOMER_ID = WATCHES_UPDATES.SK_CUSTOMER_ID AND FACT_WATCHES.SK_SECURITY_ID = WATCHES_UPDATES.SK_SECURITY_ID
WHEN MATCHED AND WATCHES_UPDATES.ACTION = 'CNCL' THEN UPDATE SET
    FACT_WATCHES.SK_DATE_ID_DATE_REMOVED = COALESCE(WATCHES_UPDATES.SK_DATE_ID_DATE_REMOVED,FACT_WATCHES.SK_DATE_ID_DATE_REMOVED),
    FACT_WATCHES.BATCH_ID = COALESCE(WATCHES_UPDATES.BATCH_ID,FACT_WATCHES.BATCH_ID)
WHEN NOT MATCHED AND WATCHES_UPDATES.ACTION = 'ACTV' THEN INSERT
    ( 
      SK_CUSTOMER_ID,
      SK_SECURITY_ID,
      SK_DATE_ID_DATE_PLACED,
      SK_DATE_ID_DATE_REMOVED,
      BATCH_ID
    )
VALUES
    ( 
      WATCHES_UPDATES.SK_CUSTOMER_ID,
      WATCHES_UPDATES.SK_SECURITY_ID,
      WATCHES_UPDATES.SK_DATE_ID_DATE_PLACED,
      NULL,
      WATCHES_UPDATES.BATCH_ID
    )
;