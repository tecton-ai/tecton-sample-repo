from tecton import HiveDSConfig, BatchDataSource


users_batch = BatchDataSource(
    name='users_batch',
    batch_ds_config=HiveDSConfig(
        database='fraud',
        table='fraud_users'
    ),
    family='core',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)
