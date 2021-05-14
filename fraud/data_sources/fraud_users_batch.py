from tecton import HiveDSConfig, BatchDataSource

fraud_users_batch = BatchDataSource(
    name="fraud_users_batch",
    batch_ds_config=HiveDSConfig(
        database='fraud',
        table='fraud_users'
    ),
    family="fraud_detection",
    owner="matt@tecton.ai",
    tags={"release": "production"}
)
