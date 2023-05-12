-- CREATE TABLE STATEMENT

CREATE OR REPLACE TABLE TPCDI_WH.PUBLIC.DIM_TIME ( TIME_ID NUMBER NOT NULL COMMENT 'SURROGATE KEY FOR THE TIME', TIME_VALUE TIME NOT NULL COMMENT 'THE TIME STORED APPROPRIATELY FOR DOING COMPARISONS IN THE DATA WAREHOUSE', HOUR_ID NUMBER NOT NULL COMMENT 'HOUR NUMBER AS A NUMBER, E.G. 01', HOUR_DESC VARCHAR(20) NOT NULL COMMENT 'HOUR NUMBER AS TEXT, E.G. 01', MINUTE_ID NUMBER NOT NULL COMMENT 'MINUTE AS A NUMBER, E.G. 23', MINUTE_DESC VARCHAR(20) NOT NULL COMMENT 'MINUTE AS TEXT, E.G. 01:23', SECOND_ID NUMBER NOT NULL COMMENT 'SECOND AS A NUMBER, E.G. 45', SECOND_DESC VARCHAR(20) NOT NULL COMMENT 'SECOND AS TEXT, E.G. 01:23:45', MARKET_HOURS_FLAG BOOLEAN COMMENT 'INDICATES A TIME DURING MARKET HOURS', OFFICE_HOURS_FLAG BOOLEAN COMMENT 'INDICATES A TIME DURING OFFICE HOURS' ) ;