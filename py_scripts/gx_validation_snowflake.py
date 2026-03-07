import great_expectations as gx
import os

# COnext -->reate and add datasource -->create amd add asset -->Add batch_request -->Add validator--> Add expectaions
#https://docs.greatexpectations.io/docs/0.18/oss/guides/connecting_to_your_data/fluent/database/connect_sql_source_data/?sql-database-type=snowflake#create-a-snowflake-data-source

from great_expectations.data_context import FileDataContext
context:FileDataContext = gx.get_context() 

# Initialise env variables
SNOWFLAKE_USER= os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD= os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_DATABASE= os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_ACCOUNT= os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_SCHEMA= os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_WAREHOUSE= os.getenv('SNOWFLAKE_WAREHOUSE')

# generate connection string
my_connection_string = f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role=accountadmin"
#print(my_connection_string)

#  Snowflake Data source
datasource_name = "my_snowflake_datasource"
datasource = context.data_sources.add_snowflake(
    name=datasource_name, 
    connection_string=my_connection_string
)

# Create Asset
asset_name = "snowflake_raw_bank_data"
asset_table_name = "BANK_MARKETING_DATA"
table_asset = datasource.add_table_asset(name=asset_name, table_name=asset_table_name)

# See what tables GX actually discovered
datasource = context.get_datasource(datasource_name)
assets = datasource.get_asset_names()
print("Available assets:", [asset for asset in assets])


batch_request_dict = {
    "datasource_name": datasource_name,
    "data_connector_name": "default_inferred_data_connector_name", 
    "data_asset_name": asset_name
}

from great_expectations.core.batch import BatchRequest
batch_request = BatchRequest(**batch_request_dict)

validator = context.get_validator( 
    datasource_name=datasource_name,
    data_asset_name=asset_name
)
validator.head()


# 1. Basic table checks
validator.expect_table_row_count_to_be_between(10000, 50000)
validator.expect_table_columns_to_match_ordered_list(["age", "job", "marital", "education", "balance", "housing", "loan", "contact", "day", "month", "duration", "campaign", "pdays", "previous", "poutcome", "y"])

# 2. Critical column validations
validator.expect_column_values_to_not_be_null("age")
validator.expect_column_values_to_be_between("age", min_value=18, max_value=100)
validator.expect_column_values_to_not_be_null("job")

# 3. Categorical checks
validator.expect_column_values_to_be_in_set("marital", value_set=["single", "married", "divorced"])
validator.expect_column_values_to_be_in_set("housing", value_set=["yes", "no"])
validator.expect_column_values_to_be_in_set("loan", value_set=["yes", "no"])

# 4. Numeric ranges
validator.expect_column_values_to_be_between("balance", min_value=-5000, max_value=1000000)
validator.expect_column_median_to_be_between("duration", 100, 2000)

print("✅ Expectations added!")

# Validate CURRENT expectations (no suite loading needed)
results = validator.validate()  # Uses expectations already added to validator
print(f"✅ Results: {results.success}")