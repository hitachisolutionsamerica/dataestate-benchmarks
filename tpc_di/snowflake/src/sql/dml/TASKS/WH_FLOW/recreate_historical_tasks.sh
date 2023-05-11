#!/bin/bash

db=tpcdi_wh
wh=snowpipe
connection=tpcdi

snowsql -c $connection -d $db -w $wh -f CREATE-DIM_REFERENCE_HISTORICAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-DIM_CUSTOMER_HISTORICAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-DIM_BROKER_HISTORICAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-DIM_COMPANY_HISTORICAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-DIM_ACCOUNT_HISTORICAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-DIM_SECURITY_HISTORICAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-DIM_FINANCIAL_HISTORICAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-FACT_PROSPECT_HISTORICAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-FACT_CASH_BALANCES_HISTORICAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-FACT_MARKET_HISTORY_HISTORICAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-CHECK_STREAM_CTRL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-FACT_WATCHES_HISTORICAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-DIM_TRADE_HISTORICAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-FACT_HOLDINGS_HISTORICAL_TSK.sql