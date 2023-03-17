from tecton import HiveConfig, KinesisConfig, StreamSource, BatchSource, DatetimePartitionColumn, PushSource
from datetime import timedelta
from tecton.types import Field, Int64, String, Timestamp



ad_impressions_hive_config = HiveConfig(
        database='demo_ads',
        table='impressions_batch',
        timestamp_field='timestamp',
        datetime_partition_columns = [
            DatetimePartitionColumn(column_name="datestr", datepart="date", zero_padded=True)
        ]
    )


ad_impressions_batch = BatchSource(
    name='ad_impressions_batch',
    batch_config=ad_impressions_hive_config,
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)



def ad_stream_translator(df):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType
    from pyspark.sql.functions import from_json, col, from_utc_timestamp, when

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

    return (
      df.selectExpr('cast (data as STRING) jsonData')
      .select(from_json('jsonData', payload_schema).alias('payload'))
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


ad_impressions_stream = StreamSource(
    name='ad_impressions_stream',
    stream_config=KinesisConfig(
        stream_name='ad-impressions-2',
        region='us-west-2',
        post_processor=ad_stream_translator,
        timestamp_field='timestamp',
        watermark_delay_threshold=timedelta(hours=24),
        initial_stream_position='trim_horizon',
        deduplication_columns=[],
        options={'roleArn': 'arn:aws:iam::472542229217:role/demo-cross-account-kinesis-ro'}
    ),
    batch_config=ad_impressions_hive_config,
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)


input_schema = [
    Field(name='content_keyword', dtype=String),
    Field(name='timestamp', dtype=Timestamp),
    Field(name='clicked', dtype=Int64),
]
keyword_click_source = PushSource(
    name="keyword_click_source",
    schema=input_schema,
    batch_config=ad_impressions_hive_config,
    description="""
        A push source for synchronous, online ingestion of ad-click events with content keyword metadata. Contains a 
        batch config for backfilling and offline training data generation.
    """,
    owner="pooja@tecton.ai",
    tags={'release': 'staging'}
)

user_schema = [
    Field(name='user_id', dtype=String),
    Field(name='timestamp', dtype=Timestamp),
    Field(name='clicked', dtype=Int64),
]
user_click_push_source = PushSource(
    name="user_event_source",
    schema=user_schema,
    description="A push source for synchronous, online ingestion of ad-click events with user info.",
    owner="pooja@tecton.ai",
    tags={'release': 'staging'}
)

