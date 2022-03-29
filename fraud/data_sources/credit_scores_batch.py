from tecton import HiveDSConfig, BatchDataSource, DatetimePartitionColumn


credit_scores_batch = BatchDataSource(
    name='credit_scores_batch',
    batch_ds_config=HiveDSConfig(
        database='demo_fraud',
        table='credit_scores',
        timestamp_column_name='timestamp',
        datetime_partition_columns = [
            DatetimePartitionColumn(column_name="date", datepart="date", zero_padded=True)
        ]
    ),
    family='fraud_detection',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)
