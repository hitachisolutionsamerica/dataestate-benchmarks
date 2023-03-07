-- INSERT INTO TABLE

INSERT INTO TPCDI_STG.PUBLIC.DAILYMARKET_STG
SELECT
   TO_CHAR(NULL) $1
  ,ROW_NUMBER() OVER (ORDER BY 1) $2
  ,$1 $3
  ,$2 $4
  ,$3 $5
  ,$4 $6
  ,$5 $7
  ,$6 $8
FROM @TPCDI_FILES/load/daily_market/DailyMarket01.txt (FILE_FORMAT => 'TXT_PIPE')
;
