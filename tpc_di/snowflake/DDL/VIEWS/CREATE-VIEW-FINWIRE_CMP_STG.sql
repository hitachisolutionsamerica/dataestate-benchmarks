CREATE OR REPLACE VIEW TPCDI_STG.PUBLIC.FINWIRE_CMP_STG AS
SELECT
    TO_TIMESTAMP_NTZ(PTS,'YYYYMMDD-HH24MISS') AS PTS,
    REC_TYPE,
    COMPANY_NAME,
    CIK,
    STATUS,
    INDUSTRY_ID,
    SP_RATING,
    TRY_TO_DATE(FOUNDING_DATE) AS FOUNDING_DATE,
    ADDR_LINE1,
    ADDR_LINE2,
    POSTAL_CODE,
    CITY,
    STATE_PROVINCE,
    COUNTRY,
    CEO_NAME,
    DESCRIPTION
FROM TPCDI_STG.PUBLIC.FINWIRE_STG
WHERE REC_TYPE = 'CMP'
;