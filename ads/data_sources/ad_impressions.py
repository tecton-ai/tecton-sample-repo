from tecton import HiveDSConfig, KinesisDSConfig, StreamDataSource, BatchDataSource

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

ad_impressions_hiveds = HiveDSConfig(
        database='demo_ads',
        table='impressions_batch',
        timestamp_column_name='timestamp',
        date_partition_column='datestr'
    )


ad_impressions_stream = StreamDataSource(
    name='ad_impressions_stream',
    stream_ds_config=KinesisDSConfig(
        stream_name='ad-impressions-2',
        region='us-west-2',
        raw_stream_translator=ad_stream_translator,
        timestamp_key='timestamp',
        default_watermark_delay_threshold='24 hours',
        default_initial_stream_position='trim_horizon',
        deduplication_columns=[],
        options={'roleArn': 'arn:aws:iam::472542229217:role/demo-cross-account-kinesis-ro'}
    ),
    batch_ds_config=ad_impressions_hiveds,
    family='ads',
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)

ad_impressions_batch = BatchDataSource(
    name='ad_impressions_batch',
    batch_ds_config=ad_impressions_hiveds,
    family='ads',
    tags={
        'release': 'production',
        'source': 'mobile'
    }
)
