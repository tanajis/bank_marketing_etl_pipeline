-------------------------------------------------------------
-- This DDL Create Raw layer tables.
-------------------------------------------------------------

USE DATABASE BANK_MARKETING;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE BANK_MARKETING.RAW.bank_marketing_data (
    age INTEGER,
    job STRING,
    marital STRING,
    education STRING,
    default_ STRING,
    balance INTEGER,
    housing STRING,
    loan STRING,
    contact STRING,
    day INTEGER,
    month STRING,
    duration INTEGER,
    campaign INTEGER,
    pdays INTEGER,
    previous INTEGER,
    poutcome STRING,
    deposit STRING
);