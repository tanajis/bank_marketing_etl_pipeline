import snowflake.connector
import os
from snowflake.connector import ProgrammingError
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SnowflakeDataLoader:
    def __init__(self, config):
        """
        Initialize Snowflake connection for Bank Marketing data pipeline
        :param self: Description
        :param config: Description
        """

        self.config = config
        self.conn = None 
        self.cursor = None

    
    def connect(self, config):
        """
        Esatblish Snowflake connection.
        
        :param self: Description
        """

        self.conn = snowflake.connector.connect(
            user = config['user'],
            account = config['account'],
            warehouse = config['warehouse'],
            database = config['database'],
            schema = config['schema'],
            password = config['password']
        )

        self.cursor = self.conn.cursor()
        logger.info("Connected to Snowflake")

    def exec_file_put(self, local_file_path, stage_name):
         
        """Upload CSV file to Snowflake stage using PUT command"""
        
        try:

            put_cammand = f"""
            PUT file://{local_file_path} @{stage_name} 
            OVERWRITE=TRUE AUTO_COMPRESS=TRUE """

            self.cursor.execute(put_cammand)

            logger.info(f"Uploaded {local_file_path} to stage")
        
            # List files in stage to verify
            # self.cursor.execute(f"LIST @{stage_name}")
        except ProgrammingError as e:
            logger.error(f"❌ Upload error: {e}") 
            raise

    def execute_sql_file(self, sql_file_path):
        """
        Execute all SQL statements from a .sql file using execute_stream
        """
        try:
            if not os.path.exists(sql_file_path):
                raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
        
            logger.info(f"Executing SQL file: {sql_file_path}")
            
            # execute_stream reads directly from the file object iterator
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                success_count = 0
                statement_count = 0
                
                self.connect(self.config)
                for cursor in self.conn.execute_stream(f):
                    statement_count += 1
                    try:
                        # Fetch results if any (SELECT statements)
                        results = cursor.fetchall()
                        if results:
                            logger.info(f"Statement {statement_count}: {len(results)} rows returned")
                        else:
                            logger.info(f"Statement {statement_count}: Executed successfully")
                        success_count += 1
                    except ProgrammingError as e:
                        logger.error(f"❌ Statement {statement_count} failed: {e}")
                    finally:
                        cursor.close()
            
            logger.info(f"SQL file complete: {success_count}/{statement_count} statements successful")
            
        except Exception as e:
            logger.error(f"SQL file execution error: {e}")
            raise
        finally:
            if self.conn:
                self.conn.commit()

    def load_data_to_sf_stage(self,local_file_path, stage_name):
            self.connect(self.config)
            self.exec_file_put(local_file_path, stage_name)
            