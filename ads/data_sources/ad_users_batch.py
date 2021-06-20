from tecton import HiveDSConfig, BatchDataSource


ad_users_batch = BatchDataSource(
    name='ad_users_batch',
    batch_ds_config=HiveDSConfig(
        database='demo_ads',
        table='users',
        # timestamp_column_name='signup_date',
    ),
    family='ads',
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)
