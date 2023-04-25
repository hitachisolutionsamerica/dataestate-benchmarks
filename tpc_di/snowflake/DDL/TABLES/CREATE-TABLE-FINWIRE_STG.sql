-- CREATE TABLE STATEMENT

CREATE OR REPLACE TABLE TPCDI_STG.PUBLIC.FINWIRE_STG ( PTS VARCHAR(15) NOT NULL COMMENT 'POSTING DATE & TIME AS YYYYMMDD-HHMMSS - FIN, CMP, SEC', REC_TYPE VARCHAR(3) NOT NULL COMMENT '“FIN”, "CMP", OR "SEC" - FIN, CMP, SEC', COMPANY_NAME VARCHAR(60) NOT NULL COMMENT 'NAME OF THE COMPANY - CMP', CIK VARCHAR(10) NOT NULL COMMENT 'COMPANY IDENTIFICATION CODE FROM SEC - CMP', STATUS VARCHAR(4) NOT NULL COMMENT 'ACTV FOR ACTIVE COMPANY, INAC FOR INACTIVE - CMP, SEC', INDUSTRY_ID VARCHAR(2) NOT NULL COMMENT 'CODE FOR INDUSTRY SEGMENT - CMP', SP_RATING VARCHAR(4) NOT NULL COMMENT 'S&P RATING - CMP', FOUNDING_DATE VARCHAR(8) COMMENT 'A DATE AS YYYYMMDD - CMP', ADDR_LINE1 VARCHAR(80) NOT NULL COMMENT 'MAILING ADDRESS - CMP', ADDR_LINE2 VARCHAR(80) COMMENT 'MAILING ADDRESS - CMP', POSTAL_CODE VARCHAR(12) NOT NULL COMMENT 'MAILING ADDRESS - CMP', CITY VARCHAR(25) NOT NULL COMMENT 'MAILING ADDRESS - CMP', STATE_PROVINCE VARCHAR(20) NOT NULL COMMENT 'MAILING ADDRESS - CMP', COUNTRY VARCHAR(24) COMMENT 'MAILING ADDRESS - CMP', CEO_NAME VARCHAR(46) NOT NULL COMMENT 'NAME OF COMPANY CEO - CMP', DESCRIPTION VARCHAR(150) NOT NULL COMMENT 'DESCRIPTION OF THE COMPANY - CMP', YEAR VARCHAR(4) NOT NULL COMMENT 'YEAR OF THE QUARTER END. - FIN', QUARTER VARCHAR(1) NOT NULL COMMENT 'QUARTER NUMBER: VALID VALUES ARE 1, 2, 3, 4 - FIN', QTR_START_DATE VARCHAR(8) NOT NULL COMMENT 'START DATE OF QUARTER, AS YYYYMMDD - FIN', POSTING_DATE VARCHAR(8) NOT NULL COMMENT 'POSTING DATE OF QUARTERLY REPORT AS YYYYMMDD - FIN', REVENUE VARCHAR(17) NOT NULL COMMENT 'REPORTED REVENUE FOR THE QUARTER - FIN', EARNINGS VARCHAR(17) NOT NULL COMMENT 'NET EARNINGS REPORTED FOR THE QUARTER - FIN', EPS VARCHAR(12) NOT NULL COMMENT 'BASIC EARNINGS PER SHARE FOR THE QUARTER - FIN', DILUTED_EPS VARCHAR(12) NOT NULL COMMENT 'DILUTED EARNINGS PER SHARE FOR THE QUARTER - FIN', MARGIN VARCHAR(12) NOT NULL COMMENT 'PROFIT DIVIDED BY REVENUES FOR THE QUARTER - FIN', INVENTORY VARCHAR(17) NOT NULL COMMENT 'VALUE OF INVENTORY ON HAND AT END OF QUARTER - FIN', ASSETS VARCHAR(17) NOT NULL COMMENT 'VALUE OF TOTAL ASSETS AT THE END OF QUARTER - FIN', LIABILITIES VARCHAR(17) NOT NULL COMMENT 'VALUE OF TOTAL LIABILITIES AT THE END OF QUARTER - FIN', SH_OUT VARCHAR(13) NOT NULL COMMENT 'AVERAGE NUMBER OF SHARES OUTSTANDING - FIN, SEC', DILUTED_SH_OUT VARCHAR(13) NOT NULL COMMENT 'AVERAGE NUMBER OF SHARES OUTSTANDING (DILUTED) - FIN', CO_NAME_OR_CIK VARCHAR(60) NOT NULL COMMENT 'COMPANY CIK NUMBER (IF ONLY DIGITS, 10 VARCHARS) OR NAME (IF NOT ONLY DIGITS, 60 VARCHARS) - FIN, SEC', SYMBOL VARCHAR(15) NOT NULL COMMENT 'SECURITY SYMBOL - SEC', ISSUE_TYPE VARCHAR(6) NOT NULL COMMENT 'ISSUE TYPE - SEC', NAME VARCHAR(70) NOT NULL COMMENT 'SECURITY NAME - SEC', EX_ID VARCHAR(6) NOT NULL COMMENT 'ID OF THE EXCHANGE THE SECURITY IS TRADED ON - SEC', FIRST_TRADE_DATE VARCHAR(8) NOT NULL COMMENT 'DATE OF FIRST TRADE AS YYYYMMDD - SEC', FIRST_TRADE_EXCHG VARCHAR(8) NOT NULL COMMENT 'DATE OF FIRST TRADE ON EXCHANGE AS YYYYMMDD - SEC', DIVIDEND VARCHAR(12) NOT NULL COMMENT 'DIVIDEND AS VALUE_T - SEC' ) ;

CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.FINWIRE_STG_CMP_STM
ON TABLE TPCDI_STG.PUBLIC.FINWIRE_STG
;

CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.FINWIRE_STG_FIN_STM
ON TABLE TPCDI_STG.PUBLIC.FINWIRE_STG
;

CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.FINWIRE_STG_SEC_STM
ON TABLE TPCDI_STG.PUBLIC.FINWIRE_STG
;
