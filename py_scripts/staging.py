from snowflake_util import SnowflakeDataLoader
import os
from pathlib import Path
import traceback
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent  # Go up 2 levels from scripts/

# Configuration - Update with your Snowflake credentials
config = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

logger.info(config)

# Usage
if __name__ == "__main__":

    csv_file_path = BASE_DIR / "data" / "bank_marketing_data.csv"
    raw_ddl_file = BASE_DIR / "sql_scripts" / "raw_table_ddl.sql"
    load_raw_tables_file = BASE_DIR / "sql_scripts" / "raw_load_tables.sql"
    raw_stage_name = 'RAW_BANK_DATA'
    try:
        # Validate file exists
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"❌ CSV file not found: {csv_file_path}")

        logger.info(f"Laoding Data From File to Stage: {csv_file_path}")

        sf_data_loader = SnowflakeDataLoader(config)
        
        # 1. Load data to Snowflake Stage Object. 
        logger.info(f"Loading data from local csv to stage...")
        sf_data_loader.load_data_to_sf_stage(csv_file_path, raw_stage_name)

        # 2. Create raw layer tables
        logger.info(f"Creating raw Layer Tables...")
        sf_data_loader.execute_sql_file(raw_ddl_file)

        # 3. Load Raw table
        logger.info(f"..Copying Data From Stage to Raw Table.")
        sf_data_loader.execute_sql_file(load_raw_tables_file)

    except Exception as e:
        exc_type, exc_obj, tb = sys.exc_info()
        logger.info(f"Line {tb.tb_lineno} in {tb.tb_frame.f_code.co_filename}")
        logger.error(f"Error {e}")