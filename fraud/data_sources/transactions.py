from tecton import spark_stream_config, FileConfig, HiveConfig, KinesisConfig, StreamSource, BatchSource, DatetimePartitionColumn
import os
from datetime import timedelta
from utils import access_secret_version

batch_config = FileConfig(
    timestamp_field='timestamp',
    # Data in demo_fraud_v2.transactions lands slightly after the real wall time, e.g. the partition for Nov 4 may not
    # land until Nov 5 00:30:00. Delay materialization jobs by one hour to accommodate this upstream data delay. This
    # delay will be reflected in training data generation for batch feature views to reflect its impact on online
    # freshness.
    uri="gs://tecton-sample-data/topics/demo_fraud_v2",
    file_format="json",
    data_delay=timedelta(hours=1),
)

confluent_bootstrap_servers = os.environ.get('CONFLUENT_BOOTSTRAP_SERVERS', 'pkc-lgk0v.us-west1.gcp.confluent.cloud:9092')
confluent_topic = os.environ.get('CONFLUENT_TOPIC', 'demo_fraud_v2')

@spark_stream_config()
def fraud_stream_config(spark):
    import os
    from pyspark.sql.functions import from_json, col, from_utc_timestamp
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    confluent_api_key = access_secret_version('CONFLUENT_API_KEY')
    confluent_secret = access_secret_version('CONFLUENT_SECRET')

    payload_schema = StructType([
        StructField('user_id', StringType(), False),
        StructField('transaction_id', StringType(), False),
        StructField('category', StringType(), False),
        StructField('amt', StringType(), False),
        StructField('is_fraud', StringType(), False),
        StructField('merchant', StringType(), False),
        StructField('merch_lat', StringType(), False),
        StructField('merch_long', StringType(), False),
        StructField('timestamp', StringType(), False),
    ])

    from pyspark.sql.functions import to_date

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", confluent_bootstrap_servers)
        .option("failOnDataLoss", "false")
        .option("subscribe", confluent_topic)
        .option("startingOffsets", "latest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option(
            "kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
                confluent_api_key, confluent_secret
            ),
        )
        .option("kafka.ssl.endpoint.identification.algorithm", "https")
        .option("kafka.sasl.mechanism", "PLAIN")
        .load()
        .selectExpr("timestamp", "cast (value as STRING) jsonData")
        .select("timestamp", from_json("jsonData", payload_schema).alias("payload"))
        .select(
            col('payload.user_id').alias('user_id'),
            col('payload.transaction_id').alias('transaction_id'),
            col('payload.category').alias('category'),
            col('payload.amt').cast('double').alias('amt'),
            col('payload.is_fraud').cast('long').alias('is_fraud'),
            col('payload.merchant').alias('merchant'),
            col('payload.merch_lat').cast('double').alias('merch_lat'),
            col('payload.merch_long').cast('double').alias('merch_long'),
            from_utc_timestamp('payload.timestamp', 'UTC').alias('timestamp')
        )

    )
    watermark = "{} seconds".format(timedelta(hours=24).seconds)
    return stream_df.withWatermark("timestamp", watermark)

transactions_stream = StreamSource(
    name='transactions_stream',
    stream_config=fraud_stream_config,
    batch_config=batch_config,
    owner='david@tecton.ai',
    tags={'release': 'production'}
)

transactions_batch = BatchSource(
    name='transactions_batch',
    batch_config=batch_config,
    owner='david@tecton.ai',
    tags={'release': 'production'}
)
