CREATE OR REPLACE VIEW TPCDI_STG.PUBLIC.CUSTOMER_NEW_STG AS
SELECT
   XML:"@ActionTS"::TIMESTAMP AS ACTION_DATE
 , XML:"@ActionType"::STRING AS ACTION_TYPE
 , XML:"$"."@C_DOB"::DATE AS DATE_OF_BIRTH
 , XML:"$"."@C_GNDR"::STRING AS GENDER
 , XML:"$"."@C_ID"::NUMBER AS CUSTOMER_ID
 , XML:"$"."@C_TAX_ID"::STRING AS CUSTOMER_TAX_ID
 , XML:"$"."@C_TIER"::STRING AS CUSTOMER_TIER
 , XML:"$"."$"[0]."$"[0]."$"::STRING AS LAST_NAME
 , XML:"$"."$"[0]."$"[1]."$"::STRING AS FIRST_NAME
 , XML:"$"."$"[0]."$"[2]."$"::STRING AS MIDDLE_INITIAL
 , XML:"$"."$"[1]."$"[0]."$"::STRING AS ADDRESS_LINE_1
 , XML:"$"."$"[1]."$"[1]."$"::STRING AS ADDRESS_LINE_2
 , XML:"$"."$"[1]."$"[2]."$"::STRING AS ZIP_CODE
 , XML:"$"."$"[1]."$"[3]."$"::STRING AS CITY
 , XML:"$"."$"[1]."$"[4]."$"::STRING AS STATE_PROVINCE
 , XML:"$"."$"[1]."$"[5]."$"::STRING AS COUNTRY
 , XML:"$"."$"[2]."$"[0]."$"::STRING AS PRIMARY_EMAIL
 , XML:"$"."$"[2]."$"[1]."$"::STRING AS ALTERNATE_EMAIL
 , XML:"$"."$"[2]."$"[2]."$"[0]."$"::STRING AS PHONE_1_COUNTRY_CODE
 , XML:"$"."$"[2]."$"[2]."$"[1]."$"::STRING AS PHONE_1_AREA_CODE
 , XML:"$"."$"[2]."$"[2]."$"[2]."$"::STRING AS PHONE_1_PHONE_NUMBER
 , XML:"$"."$"[2]."$"[2]."$"[3]."$"::STRING AS PHONE_1_EXTENSION
 , XML:"$"."$"[2]."$"[3]."$"[0]."$"::STRING AS PHONE_2_COUNTRY_CODE
 , XML:"$"."$"[2]."$"[3]."$"[1]."$"::STRING AS PHONE_2_AREA_CODE
 , XML:"$"."$"[2]."$"[3]."$"[2]."$"::STRING AS PHONE_2_PHONE_NUMBER
 , XML:"$"."$"[2]."$"[3]."$"[3]."$"::STRING AS PHONE_2_EXTENSION
 , XML:"$"."$"[2]."$"[4]."$"[0]."$"::STRING AS PHONE_3_COUNTRY_CODE
 , XML:"$"."$"[2]."$"[4]."$"[1]."$"::STRING AS PHONE_3_AREA_CODE
 , XML:"$"."$"[2]."$"[4]."$"[2]."$"::STRING AS PHONE_3_PHONE_NUMBER
 , XML:"$"."$"[2]."$"[4]."$"[3]."$"::STRING AS PHONE_3_EXTENSION
 , XML:"$"."$"[3]."$"[0]."$"::STRING AS LOCAL_TAX_ID
 , XML:"$"."$"[3]."$"[1]."$"::STRING AS NATIONAL_TAX_ID
 , XML:"$"."$"[4]."@CA_ID"::STRING AS CUSTOMER_ACCOUNT_ID
 , XML:"$"."$"[4]."$"[1]."$"::STRING AS CUSTOMER_ACCOUNT_NAME
FROM TPCDI_STG.PUBLIC.CUSTOMER_MGMT_STG
WHERE XML:"@ActionType"::STRING = 'NEW'
;
