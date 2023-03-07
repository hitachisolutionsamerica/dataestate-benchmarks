CREATE OR REPLACE VIEW TPCDI_WH.PUBLIC.TABLE_ROW_COUNTS AS
SELECT 
TABLE_CATALOG AS DATABASE,
TABLE_NAME, 
ROW_COUNT
FROM TPCDI_WH.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'PUBLIC'
UNION
SELECT 
TABLE_CATALOG AS DATABASE,
TABLE_NAME, 
ROW_COUNT
FROM TPCDI_ODS.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'PUBLIC'
UNION
SELECT 
TABLE_CATALOG AS DATABASE,
TABLE_NAME, 
ROW_COUNT
FROM TPCDI_STG.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'PUBLIC'
;
