from tecton import HiveDSConfig, BatchDataSource

transactions_batch = BatchDataSource(
    name="transactions_batch",
    batch_ds_config=HiveDSConfig(
        database='fraud',
        table='fraud_transactions_pq',
        timestamp_column_name='timestamp',
    ),
    family="fraud_detection",
    owner="matt@tecton.ai",
    tags={"release": "production"}
)
