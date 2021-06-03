from tecton import HiveDSConfig, BatchDataSource


fraud_users_batch = BatchDataSource(
    name='users_batch',
    batch_ds_config=HiveDSConfig(
        database='fraud',
        table='fraud_users'
    ),
    family='fraud',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)
