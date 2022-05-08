from tecton import BatchSource, HiveConfig, DatetimePartitionColumn


users = BatchSource(
    name='users',
    batch_config=HiveConfig(
        database='demo_fraud',
        table='users',
        timestamp_field='signup_date',
        datetime_partition_columns = [
            DatetimePartitionColumn(column_name="signup_date", datepart="date", zero_padded=True)
        ]
    ),
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description='Users',
)
