-------------------------------------------------------------
-- Create Stages in raw schema
-------------------------------------------------------------

USE DATABASE BANK_MARKETING;
USE SCHEMA RAW;

CREATE STAGE IF NOT EXISTS STAGE BANK_MARKETING.RAW.RAW_BANK_DATA 
FILE_FORMAT = (
    TYPE = CSV 
    FIELD_DELIMITER = ',' 
    SKIP_HEADER = 1
);
