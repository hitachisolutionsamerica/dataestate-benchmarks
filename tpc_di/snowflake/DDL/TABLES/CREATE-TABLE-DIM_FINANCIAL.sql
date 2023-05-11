-- CREATE TABLE STATEMENT

CREATE OR REPLACE TABLE TPCDI_WH.PUBLIC.DIM_FINANCIAL ( SK_COMPANYID NUMBER(11) NOT NULL COMMENT 'COMPANY SK.', FI_YEAR NUMBER(4) NOT NULL COMMENT 'YEAR OF THE QUARTER END.', FI_QTR NUMBER(1) NOT NULL COMMENT 'QUARTER NUMBER THAT THE FINANCIAL INFORMATION IS FOR: VALID VALUES 1, 2, 3, 4.', FI_QTR_START_DATE DATE NOT NULL COMMENT 'START DATE OF QUARTER.', FI_REVENUE NUMBER(15,2) NOT NULL COMMENT 'REPORTED REVENUE FOR THE QUARTER.', FI_NET_EARN NUMBER(15,2) NOT NULL COMMENT 'NET EARNINGS REPORTED FOR THE QUARTER.', FI_BASIC_EPS NUMBER(10,2) NOT NULL COMMENT 'BASIC EARNINGS PER SHARE FOR THE QUARTER.', FI_DILUT_EPS NUMBER(10,2) NOT NULL COMMENT 'DILUTED EARNINGS PER SHARE FOR THE QUARTER.', FI_MARGIN NUMBER(10,2) NOT NULL COMMENT 'PROFIT DIVIDED BY REVENUES FOR THE QUARTER.', FI_INVENTORY NUMBER(15,2) NOT NULL COMMENT 'VALUE OF INVENTORY ON HAND AT THE END OF QUARTER.', FI_ASSETS NUMBER(15,2) NOT NULL COMMENT 'VALUE OF TOTAL ASSETS AT THE END OF THE QUARTER.', FI_LIABILITY NUMBER(15,2) NOT NULL COMMENT 'VALUE OF TOTAL LIABILITIES AT THE END OF THE QUARTER.', FI_OUT_BASIC NUMBER(12) NOT NULL COMMENT 'AVERAGE NUMBER OF SHARES OUTSTANDING (BASIC).', FI_OUT_DILUT NUMBER(12) NOT NULL COMMENT 'AVERAGE NUMBER OF SHARES OUTSTANDING (DILUTED).', INSERTED_TS TIMESTAMP_LTZ NOT NULL COMMENT 'TIMESTAMP WHEN THIS RECORD WAS INSERTED'  ) ;
