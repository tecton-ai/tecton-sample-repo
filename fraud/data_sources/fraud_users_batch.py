from tecton import HiveDSConfig, BatchDataSource, DatetimePartitionColumn


fraud_users_batch = BatchDataSource(
    name='users_batch',
    batch_ds_config=HiveDSConfig(
        database='demo_fraud',
        table='users',
        timestamp_column_name='signup_date',
        datetime_partition_columns = [
            DatetimePartitionColumn(column_name="signup_date", datepart="date", zero_padded=True)
        ]
    ),
    family='fraud',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)
