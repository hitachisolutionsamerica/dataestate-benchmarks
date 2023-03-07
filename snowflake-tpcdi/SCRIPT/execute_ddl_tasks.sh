#!/bin/bash

db=tpcdi_stg
wh=snowpipe
connection=tpcdi

snowsql -c $connection -d $db -w $wh -f CREATE-FINWIRE_STG_TASKS.sql
