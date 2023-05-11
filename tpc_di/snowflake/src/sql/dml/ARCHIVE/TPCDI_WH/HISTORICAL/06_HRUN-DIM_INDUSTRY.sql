-- DIM_INDUSTRY ONLY REQUIRES AN HISTORICAL LOAD

MERGE INTO TPCDI_WH.PUBLIC.DIM_INDUSTRY USING
(
SELECT
IN_ID
,IN_NAME
,METADATA$ACTION
,METADATA$ISUPDATE
FROM TPCDI_STG.PUBLIC.INDUSTRY_STG_STM
) INDUSTRY_STG ON TPCDI_WH.PUBLIC.DIM_INDUSTRY.IN_ID = INDUSTRY_STG.IN_ID
WHEN MATCHED AND
INDUSTRY_STG.METADATA$ACTION = 'INSERT' AND INDUSTRY_STG.METADATA$ISUPDATE = 'TRUE'
THEN UPDATE SET
DIM_INDUSTRY.IN_NAME = INDUSTRY_STG.IN_NAME
WHEN NOT MATCHED AND
INDUSTRY_STG.METADATA$ACTION = 'INSERT' AND INDUSTRY_STG.METADATA$ISUPDATE = 'FALSE'
THEN INSERT
(
  IN_ID
  ,IN_NAME
)
VALUES
(
  INDUSTRY_STG.IN_ID
  ,INDUSTRY_STG.IN_NAME
)
;
