from tecton.compat import HiveDSConfig, KinesisDSConfig, StreamDataSource, DatetimePartitionColumn

def raw_data_deserialization(df):
    from pyspark.sql.functions import col, from_json, from_utc_timestamp, when
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType, IntegerType

    payload_schema = StructType([
        StructField('amount', StringType(), False),
        StructField('nameOrig', StringType(), False),
        StructField('nameDest', StringType(), False),
        StructField('isFraud', StringType(), False),
        StructField('isFlaggedFraud', StringType(), False),
        StructField('type_CASH_IN', StringType(), False),
        StructField('type_CASH_OUT', StringType(), False),
        StructField('type_DEBIT', StringType(), False),
        StructField('type_PAYMENT', StringType(), False),
        StructField('type_TRANSFER', StringType(), False),
        StructField('timestamp', StringType(), False),
    ])

    return (
        df.selectExpr('cast (data as STRING) jsonData')
        .select(from_json('jsonData', payload_schema).alias('payload'))
        .select(
            col('payload.amount').cast('long').alias('amount'),
            col('payload.nameOrig').alias('nameOrig'),
            col('payload.nameDest').alias('nameDest'),
            col('payload.isFraud').cast('long').alias('isFraud'),
            col('payload.isFlaggedFraud').cast('long').alias('isFlaggedFraud'),
            col('payload.type_CASH_IN').cast('long').alias('type_CASH_IN'),
            col('payload.type_CASH_OUT').cast('long').alias('type_CASH_OUT'),
            col('payload.type_DEBIT').cast('long').alias('type_DEBIT'),
            col('payload.type_PAYMENT').cast('long').alias('type_PAYMENT'),
            col('payload.type_TRANSFER').cast('long').alias('type_TRANSFER'),
            from_utc_timestamp('payload.timestamp', 'UTC').alias('timestamp')
        )
    )

partition_columns = [
    DatetimePartitionColumn(column_name="partition_0", datepart="year", zero_padded=True),
    DatetimePartitionColumn(column_name="partition_1", datepart="month", zero_padded=True),
    DatetimePartitionColumn(column_name="partition_2", datepart="day", zero_padded=True),
]

transactions_stream = StreamDataSource(
    name='transactions_stream',
    stream_ds_config=KinesisDSConfig(
        stream_name='transaction_events',
        region='us-west-2',
        default_initial_stream_position='latest',
        default_watermark_delay_threshold='24 hours',
        timestamp_key='timestamp',
        raw_stream_translator=raw_data_deserialization,
        options={'roleArn': 'arn:aws:iam::472542229217:role/demo-cross-account-kinesis-ro'}
    ),
    batch_ds_config=HiveDSConfig(
        database='demo_fraud',
        table='transactions',
        timestamp_column_name='timestamp',
        # Setting the datetime partition columns significantly speeds up queries from Hive tables.
        datetime_partition_columns=partition_columns,
    ),
    family='fraud',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)
