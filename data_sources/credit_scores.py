from tecton import BatchSource, HiveConfig, DatetimePartitionColumn


credit_scores = BatchSource(
    name='credit_scores',
    batch_config=HiveConfig(
        database='demo_fraud',
        table='credit_scores',
        timestamp_field='timestamp',
        datetime_partition_columns = [
            DatetimePartitionColumn(column_name="date", datepart="date", zero_padded=True)
        ]
    ),
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description='Users credit scores batch data source'
)
