-------------------------------------------------------------
-- COPY cammand to move data from snowflake stage to raw table.
-------------------------------------------------------------

----- Re-running the cammand does not copy data that has already been loaded.

---- Actual load
COPY INTO  BANK_MARKETING.RAW.BANK_MARKETING_DATA  -- Raw Layer Table
FROM @BANK_MARKETING.RAW.RAW_BANK_DATA --- Snowflake Stage
FILE_FORMAT = (FORMAT_NAME = 'mycsv') 