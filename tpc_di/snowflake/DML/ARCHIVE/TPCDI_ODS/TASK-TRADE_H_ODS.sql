  -- LOAD SUBMITTED OR PENDING TRADES

INSERT INTO TPCDI_ODS.PUBLIC.TRADE_ODS
SELECT
    TR.T_ID
  , TH.TH_DTS
  , TH.TH_ST_ID
  , TR.T_TT_ID
  , TR.T_IS_CASH
  , TR.T_S_SYMB
  , TR.T_QTY
  , TR.T_BID_PRICE
  , TR.T_CA_ID
  , TR.T_EXEC_NAME
  , TR.T_TRADE_PRICE
  , TR.T_CHRG
  , TR.T_COMM
  , TR.T_TAX
  , CURRENT_TIMESTAMP()
FROM TPCDI_STG.PUBLIC.TRADE_STG_I_STM TR
INNER JOIN TPCDI_STG.PUBLIC.TRADEHISTORY_STG_I_STM TH
  ON TR.T_ID = TH.TH_T_ID
WHERE
  (TH.TH_ST_ID = 'SBMT' AND TR.T_TT_ID IN ('TMB','TMS'))
  OR TH.TH_ST_ID = 'PNDG'
;

-- INSERT/UPDATE COMPLETED OR CANCELLED TRADES

UPDATE TPCDI_ODS.PUBLIC.TRADE_ODS
SET


CREATE OR REPLACE TASK TPCDI_ODS.PUBLIC.TRADE_ODS_TSK
  WAREHOUSE = TASK_WH,
  SCHEDULE = '1 MINUTE'
AS


MERGE INTO TPCDI_ODS.PUBLIC.TRADE_ODS USING (
  SELECT
      TR.T_ID T_ID
    , TH.TH_DTS T_DTS
    , TH.TH_ST_ID T_ST_ID
    , TR.T_TT_ID T_TT_ID
    , TR.T_IS_CASH T_IS_CASH
    , TR.T_S_SYMB T_S_SYMB
    , TR.T_QTY T_QTY
    , TR.T_BID_PRICE T_BID_PRICE
    , TR.T_CA_ID T_CA_ID
    , TR.T_EXEC_NAME T_EXEC_NAME
    , TR.T_TRADE_PRICE T_TRADE_PRICE
    , TR.T_CHRG T_CHRG
    , TR.T_COMM T_COMM
    , TR.T_TAX T_TAX
  FROM TPCDI_STG.PUBLIC.TRADE_STG_U_STM TR
  INNER JOIN TPCDI_STG.PUBLIC.TRADEHISTORY_STG_U_STM TH
    ON TR.T_ID = TH.TH_T_ID
  WHERE
    TH.TH_ST_ID in ('CMPT','CNCL')
) TRADE_STG ON TPCDI_ODS.PUBLIC.TRADE_ODS.T_ID = TRADE_STG.T_ID
WHEN MATCHED THEN UPDATE SET
    TRADE_ODS.T_DTS = TRADE_STG.T_DTS
  , TRADE_ODS.T_ST_ID = TRADE_STG.T_ST_ID
  , TRADE_ODS.T_TT_ID = TRADE_STG.T_TT_ID
  , TRADE_ODS.T_IS_CASH = TRADE_STG.T_IS_CASH
  , TRADE_ODS.T_S_SYMB = TRADE_STG.T_S_SYMB
  , TRADE_ODS.T_QTY = TRADE_STG.T_QTY
  , TRADE_ODS.T_BID_PRICE = TRADE_STG.T_BID_PRICE
  , TRADE_ODS.T_CA_ID = TRADE_STG.T_CA_ID
  , TRADE_ODS.T_EXEC_NAME = TRADE_STG.T_EXEC_NAME
  , TRADE_ODS.T_TRADE_PRICE = TRADE_STG.T_TRADE_PRICE
  , TRADE_ODS.T_CHRG = TRADE_STG.T_CHRG
  , TRADE_ODS.T_COMM = TRADE_STG.T_COMM
  , TRADE_ODS.T_TAX = TRADE_STG.T_TAX
  , TRADE_ODS.LAST_UPDATED_TS = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
VALUES
(
    TRADE_STG.T_ID
  , TRADE_STG.T_DTS
  , TRADE_STG.T_ST_ID
  , TRADE_STG.T_TT_ID
  , TRADE_STG.T_IS_CASH
  , TRADE_STG.T_S_SYMB
  , TRADE_STG.T_QTY
  , TRADE_STG.T_BID_PRICE
  , TRADE_STG.T_CA_ID
  , TRADE_STG.T_EXEC_NAME
  , TRADE_STG.T_TRADE_PRICE
  , TRADE_STG.T_CHRG
  , TRADE_STG.T_COMM
  , TRADE_STG.T_TAX
  , CURRENT_TIMESTAMP()
)
;

ALTER TASK TPCDI_ODS.PUBLIC.TRADE_ODS_TSK RESUME;