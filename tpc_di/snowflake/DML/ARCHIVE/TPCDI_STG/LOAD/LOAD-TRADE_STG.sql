-- INSERT INTO TABLE

INSERT INTO TPCDI_STG.PUBLIC.TRADE_STG
SELECT
   TO_CHAR(NULL) $1
  ,ROW_NUMBER() OVER (ORDER BY 1) $2
  ,$1 $3
  ,$2 $4
  ,$3 $5
  ,$4 $6
  ,$5 $7
  ,$6 $8
  ,$7 $9
  ,$8 $10
  ,$9 $11
  ,$10 $12
  ,$11 $13
  ,$12 $14
  ,$13 $15
  ,$14 $16
FROM @TPCDI_FILES/load/trade/Trade01.txt (FILE_FORMAT => 'TXT_PIPE')
;
