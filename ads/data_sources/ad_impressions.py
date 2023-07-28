from tecton import spark_stream_config, FileConfig, HiveConfig, KinesisConfig, StreamSource, BatchSource, DatetimePartitionColumn, PushSource
from datetime import timedelta
from tecton.types import Field, Int64, String, Timestamp
import os
from utils import access_secret_version



ad_impressions_hive_config = FileConfig(
        timestamp_field='timestamp',
        uri="gs://tecton-sample-data/topics/ad-impressions-2",
        file_format="json",
    )


ad_impressions_batch = BatchSource(
    name='ad_impressions_batch',
    batch_config=ad_impressions_hive_config,
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)

confluent_bootstrap_servers = os.environ.get('CONFLUENT_BOOTSTRAP_SERVERS', 'pkc-lgk0v.us-west1.gcp.confluent.cloud:9092')
confluent_topic = os.environ.get('CONFLUENT_TOPIC', 'ad-impressions-2')



@spark_stream_config()
def ad_impressions_stream_config(spark):
    from pyspark.sql.functions import from_json, col, when, from_utc_timestamp
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    confluent_api_key = access_secret_version('CONFLUENT_API_KEY')
    confluent_secret = access_secret_version('CONFLUENT_SECRET')

    payload_schema = StructType([
        StructField('clicked', StringType(), False),
        StructField('auction_id', StringType(), False),
        StructField('num_ads_bid', StringType(), False),
        StructField('ad_id', StringType(), False),
        StructField('ad_campaign_id', StringType(), False),
        StructField('partner_domain_name', StringType(), False),
        StructField('content_keyword', StringType(), False),
        StructField('ad_content_id', StringType(), False),
        StructField('ad_group_id', StringType(), False),
        StructField('ad_display_placement', StringType(), False),
        StructField('ad_destination_domain_id', StringType(), False),
        StructField('partner_id', StringType(), False),
        StructField('is_pwa', StringType(), False),
        StructField('user_uuid', StringType(), False),
        StructField('timestamp', StringType(), False),
        StructField('datestr', StringType(), True),
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
            col('payload.clicked').cast('long').alias('clicked'),
            col('payload.auction_id').alias('auction_id'),
            col('payload.num_ads_bid').cast('long').alias('num_ads_bid'),
            col('payload.ad_id').cast('long').alias('ad_id'),
            col('payload.ad_campaign_id').cast('long').alias('ad_campaign_id'),
            col('payload.partner_domain_name').alias('partner_domain_name'),
            col('payload.content_keyword').alias('content_keyword'),
            col('payload.ad_content_id').cast('long').alias('ad_content_id'),
            col('payload.ad_group_id').alias('ad_group_id'),
            col('payload.ad_display_placement').alias('ad_display_placement'),
            col('payload.ad_destination_domain_id').cast('long').alias('ad_destination_domain_id'),
            col('payload.partner_id').cast('long').alias('partner_id'),
            when(
                col('payload.is_pwa') == 'True',
                True).when(
                    col('payload.is_pwa') == 'False',
                    False).alias('is_pwa'),
                col('payload.user_uuid').alias('user_uuid'),
                from_utc_timestamp('payload.timestamp', 'UTC').alias('timestamp')
        )
    )
    watermark = "{} seconds".format(timedelta(hours=24).seconds)
    return stream_df.withWatermark("timestamp", watermark)

ad_impressions_stream = StreamSource(
    name='ad_impressions_stream',
    stream_config=ad_impressions_stream_config,
    batch_config=ad_impressions_hive_config,
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)
