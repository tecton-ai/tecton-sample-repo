from tecton import HiveDSConfig, BatchDataSource, DatetimePartitionColumn

partition_columns = [
    DatetimePartitionColumn(column_name="partition_0", datepart="year", zero_padded=True),
    DatetimePartitionColumn(column_name="partition_1", datepart="month", zero_padded=True),
    DatetimePartitionColumn(column_name="partition_2", datepart="day", zero_padded=True),
]

transactions_batch = BatchDataSource(
    name='transactions_batch',
    batch_ds_config=HiveDSConfig(
        database='demo_fraud',
        table='transactions',
        timestamp_column_name='timestamp',
        # Setting the datetime partition columns significantly speeds up queries from Hive tables.
        datetime_partition_columns=partition_columns,
    ),
    family='fraud_detection',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)
