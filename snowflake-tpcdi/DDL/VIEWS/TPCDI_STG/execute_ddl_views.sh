#!/bin/bash

db=tpcdi_stg
wh=snowpipe
connection=tpcdi

for fname in *.sql
do 
  snowsql -c $connection -d $db -w $wh -f $fname -o output_format=csv -o friendly=False -o timing=False -o echo=False -o sfqid_in_error=True -o output_file=log/$fname.csv
done
