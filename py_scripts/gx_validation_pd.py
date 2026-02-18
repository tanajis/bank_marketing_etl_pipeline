import pandas as pd
import great_expectations as gx 
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
CSV_FILE_PATH = BASE_DIR / "data" / "bank_marketing_data.csv"

context = gx.get_context(mode='ephemeral') # ephemeral does not stare metata for future validations

# 1.Create Data Source(Pandas)
ds = context.data_sources.add_pandas(name="ds_raw_bank_marketing")

# 2.Create Data Asset(Dataframe)
data_asset = ds.add_dataframe_asset(name='da_raw_bank_marketing')

# 3. Batch Defination 
bd = data_asset.add_batch_definition_whole_dataframe("batch_raw_bank_marketing")

# 4. Expectations

ex_age = gx.expectations.ExpectColumnValuesToBeBetween(column="age",max_value=100, min_value=5)
ex_marital = gx.expectations.ExpectColumnDistinctValuesToBeInSet(column="marital", value_set=["married","single"])

# 5.Create Expecation Suit and Add Expecations
ex_suit = gx.ExpectationSuite(name="ex_raw_bank_marketing")
Expecation_suit_list =context.suites.add(ex_suit)
Expecation_suit_list.add_expectation(ex_age)
Expecation_suit_list.add_expectation(ex_marital)


# 6.Create Validation Definations

validation_definition_name = 'Bank Marketing Raw Layer Validation'
validation_defination_ref = gx.ValidationDefinition(data =bd, suite=Expecation_suit_list, name = validation_definition_name)
validation_defination = context.validation_definitions.add(validation_defination_ref)


# 7. Creating checkpoint

checkpoint_name ='Bank Marketing Data'
checkpoint_obj =gx.Checkpoint(name = checkpoint_name,validation_definitions = [validation_defination], actions =[], result_format= "COMPLETE")
checkpoint =  context.checkpoints.add(checkpoint_obj)

# Create Dataframe and execute the checkpoint for it.
df =pd.read_csv(CSV_FILE_PATH)
# df.head(10)
batch_param = {"dataframe": df}
result = checkpoint.run(batch_parameters=batch_param)
print(result)