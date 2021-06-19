from tecton import HiveDSConfig, BatchDataSource


transactions_batch = BatchDataSource(
    name='transactions_batch',
    batch_ds_config=HiveDSConfig(
        database='demo_fraud',
        table='fraud_transactions',
        timestamp_column_name='timestamp',
    ),
    family='fraud_detection',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)
