




gx_task = GreatExpectationsOperator(
task_id='validate_data',
expectation_suite_name='bank_marketing_suite',
data_context_root='/path/to/gx/',
conn_id='snowflake_default',
dag=dag
)

