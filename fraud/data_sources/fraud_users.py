from tecton import FileConfig, HiveConfig, BatchSource, DatetimePartitionColumn


fraud_users_batch = BatchSource(
    name='users_batch',
    batch_config=FileConfig(
        uri='gs://tecton-sample-data/tutorials/fraud/data_sources/customers.pq',
        file_format='parquet',
        timestamp_field='signup_timestamp'
    ),
    owner='david@tecton.ai',
    tags={'release': 'production'}
)
