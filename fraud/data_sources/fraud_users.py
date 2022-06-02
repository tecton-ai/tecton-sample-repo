from tecton import HiveConfig, BatchSource, DatetimePartitionColumn


fraud_users_batch = BatchSource(
    name='users_batch',
    batch_config=HiveConfig(
        database='demo_fraud_v2',
        table='customers',
        timestamp_field='signup_timestamp'
    ),
    owner='david@tecton.ai',
    tags={'release': 'production'}
)
