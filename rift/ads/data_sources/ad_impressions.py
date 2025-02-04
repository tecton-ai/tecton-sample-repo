from tecton import HiveConfig, KinesisConfig, StreamSource, BatchSource, DatetimePartitionColumn, PushConfig
from datetime import timedelta
from tecton.types import Field, Int64, String, Timestamp, Bool

import tecton
from tecton import pyarrow_batch_config, FilterContext

# Declare a pyarrow_batch_config that connects to your Iceberg table
@pyarrow_batch_config(
    supports_time_filtering=True,
)
def ad_impressions_iceberg_config(filter_context):
    from pyiceberg.catalog import load_catalog

    catalog_type = "demo_ads"
    catalog_configurations = {"type": catalog_type}  # Add any configuration params that apply
    table_name = "impressions_batch"

    catalog = load_catalog(catalog_type, **catalog_configurations)
    tbl = catalog.load_table(table_name)

    # Apply timestamp filtering if available
    filters = []
    if filter_context:
        if filter_context.start_time:
            filters.append(("timestamp_column", ">=", filter_context.start_time))
        if filter_context.end_time:
            filters.append(("timestamp_column", "<", filter_context.end_time))
    
    # Apply filters if any
    scan = tbl.scan()
    if filters:
        scan = scan.filter(filters)

    return scan.to_arrow()


# Use in the BatchSource
ad_impressions_batch = BatchSource(
    name='ad_impressions_batch',
    batch_config=ad_impressions_iceberg_config
)


ad_impressions_stream = StreamSource(
    name='ad_impressions_stream',
    stream_config=PushConfig(),
    batch_config=ad_impressions_iceberg_config,
    tags={
        'release': 'production',
        'source': 'mobile'
    },
    schema = [
        Field("clicked", dtype=Int64),              # cast('long')
        Field("auction_id", dtype=String),
        Field("num_ads_bid", dtype=Int64),          # cast('long')
        Field("ad_id", dtype=Int64),                # cast('long')
        Field("ad_campaign_id", dtype=Int64),       # cast('long')
        Field("partner_domain_name", dtype=String),
        Field("content_keyword", dtype=String),
        Field("ad_content_id", dtype=Int64),        # cast('long')
        Field("ad_group_id", dtype=String),
        Field("ad_display_placement", dtype=String),
        Field("ad_destination_domain_id", dtype=Int64),  # cast('long')
        Field("partner_id", dtype=Int64),           # cast('long')
        Field("is_pwa", dtype=Bool),                # when condition creates boolean
        Field("user_uuid", dtype=String),
        Field("timestamp", dtype=Timestamp)         # from_utc_timestamp creates timestamp
    ]
)


input_schema = [
    Field(name='content_keyword', dtype=String),
    Field(name='timestamp', dtype=Timestamp),
    Field(name='clicked', dtype=Int64),
]
keyword_click_source = StreamSource(
    name="keyword_click_source",
    schema=input_schema,
    batch_config=ad_impressions_iceberg_config,
    description="""
        A push source for synchronous, online ingestion of ad-click events with content keyword metadata. Contains a 
        batch config for backfilling and offline training data generation.
    """,
    owner="demo-user@tecton.ai",
    tags={'release': 'staging'},
    stream_config=PushConfig()
)

user_schema = [
    Field(name='user_id', dtype=String),
    Field(name='timestamp', dtype=Timestamp),
    Field(name='clicked', dtype=Int64),
]
user_click_push_source = StreamSource(
    name="user_event_source",
    schema=user_schema,
    description="A push source for synchronous, online ingestion of ad-click events with user info.",
    owner="demo-user@tecton.ai",
    tags={'release': 'staging'},
    stream_config=PushConfig()
)

