-- HISTORICAL LOAD

  -- RESET SEQUENCE
  CREATE OR REPLACE SEQUENCE TPCDI_WH.PUBLIC.DIM_BROKER_SEQ
    START WITH = 1
    INCREMENT = 1
    COMMENT = 'DATABASE SEQUENCE TO SOURCE THE SURROGATE KEY FOR BROKER.'
  ;

  -- LOAD TABLE
  INSERT INTO TPCDI_WH.PUBLIC.DIM_BROKER
  SELECT
     TPCDI_WH.PUBLIC.DIM_BROKER_SEQ.NEXTVAL 
   , EMPLOYEEID
   , MANAGERID
   , EMPLOYEEFIRSTNAME
   , EMPLOYEELASTNAME
   , EMPLOYEEMI
   , EMPLOYEEBRANCH
   , EMPLOYEEOFFICE
   , EMPLOYEEPHONE
   , TO_NUMBER(1)
   , LOCALTIMESTAMP()
  FROM TPCDI_STG.PUBLIC.HR_STG_STM
  WHERE EMPLOYEEJOBCODE = 314
  AND METADATA$ACTION = 'INSERT'
  AND METADATA$ISUPDATE = 'FALSE'
  ;