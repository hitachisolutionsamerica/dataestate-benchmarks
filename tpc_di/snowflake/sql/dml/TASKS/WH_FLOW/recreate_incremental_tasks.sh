#!/bin/bash

db=tpcdi_wh
wh=snowpipe
connection=tpcdi

snowsql -c $connection -d $db -w $wh -f CREATE-DIM_CUSTOMER_INCREMENTAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-FACT_MARKET_HISTORY_INCREMENTAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-DIM_ACCOUNT_INCREMENTAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-FACT_PROSPECT_INCREMENTAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-FACT_CASH_BALANCES_INCREMENTAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-FACT_WATCHES_INCREMENTAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-DIM_TRADE_INCREMENTAL_TSK.sql
snowsql -c $connection -d $db -w $wh -f CREATE-FACT_HOLDINGS_INCREMENTAL_TSK.sql