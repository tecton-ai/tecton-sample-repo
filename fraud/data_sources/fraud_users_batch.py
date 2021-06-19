from tecton import HiveDSConfig, BatchDataSource


fraud_users_batch = BatchDataSource(
    name='users_batch',
    batch_ds_config=HiveDSConfig(
        database='demo_fraud',
        table='fraud_users',
        timestamp_column_name='signup_date'
    ),
    family='fraud',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)
