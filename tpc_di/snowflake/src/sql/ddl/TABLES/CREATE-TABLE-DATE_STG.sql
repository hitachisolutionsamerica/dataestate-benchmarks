-- CREATE TABLE STATEMENT

CREATE OR REPLACE TABLE TPCDI_STG.PUBLIC.DATE_STG ( SK_DATEID INT NOT NULL COMMENT 'SURROGATE KEY FOR THE DATE', DATEVALUE CHAR(20) NOT NULL COMMENT 'THE DATE AS TEXT, E.G. “2004-07-07”', DATEDESC CHAR(20) NOT NULL COMMENT 'THE DATE MONTH DAY, YYYY, E.G. JULY 7, 2004', CALENDARYEARID NUMBER(4) NOT NULL COMMENT 'YEAR NUMBER AS A NUMBER', CALENDARYEARDESC CHAR(20) NOT NULL COMMENT 'YEAR NUMBER AS TEXT', CALENDARQTRID NUMBER(5) NOT NULL COMMENT 'QUARTER AS A NUMBER, E.G. 20042', CALENDARQTRDESC CHAR(20) NOT NULL COMMENT 'QUARTER AS TEXT, E.G. “2004 Q2”', CALENDARMONTHID NUMBER(6) NOT NULL COMMENT 'MONTH AS A NUMBER, E.G. 20047', CALENDARMONTHDESC CHAR(20) NOT NULL COMMENT 'MONTH AS TEXT, E.G. “2004 JULY”', CALENDARWEEKID NUMBER(6) NOT NULL COMMENT 'WEEK AS A NUMBER, E.G. 200428', CALENDARWEEKDESC CHAR(20) NOT NULL COMMENT 'WEEK AS TEXT, E.G. “2004-W28”', DAYOFWEEKNUM NUMBER(1) NOT NULL COMMENT 'DAY OF WEEK AS A NUMBER, E.G. 3', DAYOFWEEKDESC CHAR(10) NOT NULL COMMENT 'DAY OF WEEK AS TEXT, E.G. “WEDNESDAY”', FISCALYEARID NUMBER(4) NOT NULL COMMENT 'FISCAL YEAR AS A NUMBER, E.G. 2005', FISCALYEARDESC CHAR(20) NOT NULL COMMENT 'FISCAL YEAR AS TEXT, E.G. “2005”', FISCALQTRID NUMBER(5) NOT NULL COMMENT 'FISCAL QUARTER AS A NUMBER, E.G. 20051', FISCALQTRDESC CHAR(20) NOT NULL COMMENT 'FISCAL QUARTER AS TEXT, E.G. “2005 Q1”', HOLIDAYFLAG BOOLEAN COMMENT 'INDICATES HOLIDAYS' ) ;

CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.DATE_STG_STM
ON TABLE TPCDI_STG.PUBLIC.DATE_STG
;
