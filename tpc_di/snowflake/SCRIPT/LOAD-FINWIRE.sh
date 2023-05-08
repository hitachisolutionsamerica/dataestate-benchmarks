#!/bin/bash

db=tpcdi_stg
wh=snowpipe
connection=tpcdi
schema=public
year=2017
sleepsecs=55
maxyear=2017

snowsql -c $connection -d $db -s $schema -w $wh -q 'ALTER TASK TPCDI_WH.PUBLIC.DIM_COMPANY_TSK RESUME;'
snowsql -c $connection -d $db -s $schema -w $wh -q 'ALTER TASK TPCDI_WH.PUBLIC.DIM_FINANCIAL_TSK RESUME;'
snowsql -c $connection -d $db -s $schema -w $wh -q 'ALTER TASK TPCDI_WH.PUBLIC.DIM_SECURITY_TSK RESUME;'
echo "All tasks resumed."
echo "Waiting 75 seconds..."
sleep 75

while [ $year -le $maxyear ]
do
  for q in {1..3}
    do
      echo "Loading FINWIRE${year}Q${q} file..."
      snowsql -c $connection -d $db -s $schema -w $wh -q 'COPY INTO TPCDI_STG.PUBLIC.FINWIRE_STG FROM (SELECT SUBSTR($1, 0, 15) PTS, SUBSTR($1, 16, 3) REC_TYPE, SUBSTR($1, 19, 60) COMPANY_NAME, SUBSTR($1, 79, 10) CIK, IFF(SUBSTR($1, 16, 3) = '\''CMP'\'', SUBSTR($1, 89, 4),SUBSTR($1, 40, 4))  STATUS, SUBSTR($1, 93, 2) INDUSTRY_ID, SUBSTR($1, 95, 4) SP_RATING, SUBSTR($1, 99, 8) FOUNDING_DATE, SUBSTR($1, 107, 80) ADDR_LINE1, SUBSTR($1, 187, 80) ADDR_LINE2, SUBSTR($1, 267, 12) POSTAL_CODE, SUBSTR($1, 279, 25) CITY, SUBSTR($1, 304, 20) STATE_PROVINCE,  SUBSTR($1, 324, 24) COUNTRY, SUBSTR($1, 348, 46) CEO_NAME, SUBSTR($1, 394, 150) DESCRIPTION, SUBSTR($1, 19, 4) YEAR, SUBSTR($1, 23, 1) QUARTER, SUBSTR($1, 24, 8) QTR_START_DATE, SUBSTR($1, 32, 8) POSTING_DATE, SUBSTR($1, 40, 17) REVENUE, SUBSTR($1, 57, 17) EARNINGS, SUBSTR($1, 74, 12) EPS, SUBSTR($1, 86, 12) DILUTED_EPS, SUBSTR($1, 98, 12) MARGIN, SUBSTR($1, 110, 17) INVENTORY, SUBSTR($1, 127, 17) ASSETS, SUBSTR($1, 144, 17) LIABILITIES, IFF(SUBSTR($1, 16, 3) = '\''FIN'\'', SUBSTR($1, 161, 13), SUBSTR($1, 120, 13)) SH_OUT, SUBSTR($1, 174, 13) DILUTED_SH_OUT, IFF(SUBSTR($1, 16, 3) = '\''FIN'\'', SUBSTR($1, 187, 60), SUBSTR($1, 161, 60)) CO_NAME_OR_CIK, SUBSTR($1, 19, 15) SYMBOL, SUBSTR($1, 34, 6) ISSUE_TYPE, SUBSTR($1, 44, 70) NAME, SUBSTR($1, 114, 6) EX_ID, SUBSTR($1, 133, 8) FIRST_TRADE_DATE, SUBSTR($1, 141, 8) FIRST_TRADE_EXCHG, SUBSTR($1, 149, 12) DIVIDEND FROM @TPCDI_FILES/load/finwire/FINWIRE'"${year}"'Q'"${q}"') FILE_FORMAT = (FORMAT_NAME = '\''TXT_FIXED_WIDTH'\'');'
      echo "Waiting $sleepsecs seconds..."
      sleep $sleepsecs
    done
    ((year++))
done

echo "All files loaded."
echo "Waiting $sleepsecs seconds..."
snowsql -c $connection -d $db -s $schema -w $wh -q 'ALTER TASK TPCDI_WH.PUBLIC.DIM_COMPANY_TSK SUSPEND;'
snowsql -c $connection -d $db -s $schema -w $wh -q 'ALTER TASK TPCDI_WH.PUBLIC.DIM_FINANCIAL_TSK SUSPEND;'
snowsql -c $connection -d $db -s $schema -w $wh -q 'ALTER TASK TPCDI_WH.PUBLIC.DIM_SECURITY_TSK SUSPEND;'
echo "All tasks suspended."
echo "Load complete."
