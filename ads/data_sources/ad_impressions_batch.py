from tecton import HiveDSConfig, BatchDataSource


ad_impressions_batch = BatchDataSource(
    name='ad_impressions_batch',
    batch_ds_config=HiveDSConfig(
        database='demo_ads',
        table='impressions_batch',
        timestamp_column_name='timestamp',
        date_partition_column='datestr'
    ),
    family='ads',
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)
