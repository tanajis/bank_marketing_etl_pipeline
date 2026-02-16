
Step1 : Create Database stage Schema 

CREATE WAREHOUSE BANK_WH WITH WAREHOUSE_SIZE = 'XSMALL';
GRANT USAGE ON DATABASE BANK_MARKETING TO ROLE ACCOUNTADMIN

CREATE DATABASE BANK_MARKETING;
CREATE SCHEMA RAW;
CREATE SCHEMA ANALYTICS;
--------------------------------

USE DATABASE BANK_MARKETING; 
USE SCHEMA RAW; 
-- Create STage 
CREATE STAGE RAW_BANK_DATA 
FILE_FORMAT = (
    TYPE = CSV 
    FIELD_DELIMITER = ',' 
    SKIP_HEADER = 1
);

SHOW STAGES;
-----------------------------
Load data to stage:

 py .\Scripts\staging.py

SHOW STAGES;
Check if data is loaded to the stage
LIST @BANK_MARKETING.RAW.RAW_BANK_DATA

--------------------------------------------------
Now from stage copy data into table

CREATE TABLE raw.bank_marketing (
age INTEGER NOT NULL,
job VARCHAR(50),
marital VARCHAR(20),
education VARCHAR(50),
default VARCHAR(10),
balance INTEGER,
housing VARCHAR(10),
loan VARCHAR(10),
contact VARCHAR(20),
day INTEGER,
month VARCHAR(10),
duration INTEGER,
campaign INTEGER,
pdays INTEGER,
previous INTEGER,
poutcome VARCHAR(20),
deposit VARCHAR(10)
);


COPY INTO RAW.bank_marketing FROM @RAW_BANK_DATA FILE_FORMAT = (TYPE = CSV);



----------------------





Cammand to set env variable in powershell:

Get-Content .env | ForEach-Object { if ($_ -match "^([^=]+)=(.*)$") { [Environment]::SetEnvironmentVariable($matches[1], $matches[2], "Process") } }