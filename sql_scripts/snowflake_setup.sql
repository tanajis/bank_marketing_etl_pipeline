-------------------------------------------------------------
-- This DDL Schema and warehouses
-------------------------------------------------------------

CREATE WAREHOUSE BANK_WH WITH WAREHOUSE_SIZE = 'XSMALL';
GRANT USAGE ON DATABASE BANK_MARKETING TO ROLE ACCOUNTADMIN
---------------------------------------------------
CREATE DATABASE BANK_MARKETING;
CREATE SCHEMA RAW;
CREATE SCHEMA ANALYTICS;
---------------------------------------------------