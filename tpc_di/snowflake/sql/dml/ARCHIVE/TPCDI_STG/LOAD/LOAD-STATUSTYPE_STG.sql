-- COPY INTO TABLE

COPY INTO TPCDI_STG.PUBLIC.STATUSTYPE_STG
FROM @TPCDI_FILES/load/status_type/StatusType01.txt
FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE')
;