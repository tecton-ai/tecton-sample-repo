from datetime import timedelta, datetime
from tecton import stream_feature_view, FilteredSource, RedisConfig, Aggregation, BatchTriggerType
from tecton.types import Field, Int64, String, Timestamp
from ads.entities import content_keyword
from ads.data_sources.ad_impressions import keyword_push_source_redis

output_schema = [
    Field(name='content_keyword', dtype=String),
    Field(name='timestamp', dtype=Timestamp),
    Field(name='clicked', dtype=Int64),
]

@stream_feature_view(
    name="keyword_push_redis_wafv",
    source=FilteredSource(keyword_push_source_redis),
    entities=[content_keyword],
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    batch_schedule=timedelta(days=1),
    online_store=RedisConfig(),
    batch_trigger=BatchTriggerType.MANUAL,
    tags={'release': 'production'},
    owner='poojal@tecton.ai',
    description='The ad clicks for a content keyword',
    mode='python',
    aggregations=[
        Aggregation(column='clicked', function='sum', time_window=timedelta(hours=1)),
        Aggregation(column='clicked', function='sum', time_window=timedelta(hours=24)),
        Aggregation(column='clicked', function='sum', time_window=timedelta(hours=72)),
        Aggregation(column='clicked', function='sum', time_window=timedelta(days=7)),
    ],
    schema=output_schema,
)
def keyword_push_redis_wafv(click_push_source):
    click_push_source["clicked"] = click_push_source["clicked"] ** 2
    return click_push_source
