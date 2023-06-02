from datetime import timedelta, datetime
from tecton import DatabricksClusterConfig
from tecton import StreamFeatureView, FilteredSource, stream_feature_view, BatchTriggerType, Aggregation
from tecton.types import Field, String, Timestamp, Int64
from ads.entities import content_keyword
from ads.data_sources.ad_impressions import keyword_click_source
from pandas import Timestamp as pdtimestamp


output_schema = [
    Field(name='content_keyword', dtype=String),
    Field(name='timestamp', dtype=Timestamp),
    Field(name='clicked', dtype=Int64),
]

db_config = DatabricksClusterConfig(
    dbr_version="10.4.x-scala2.12",
    spark_config={"spark.sql.execution.arrow.pyspark.enabled": "false"}
)


@stream_feature_view(
    name="pandas_udf",
    source=FilteredSource(keyword_click_source),
    entities=[content_keyword],
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=30),
    tags={'release': 'production'},
    owner='achal@tecton.ai',
    description='The ad clicks for a user',
    mode='pandas',
    schema=output_schema,
    batch_trigger=BatchTriggerType.SCHEDULED,
    batch_compute=db_config,
)
def content_keyword_click_counts_pandas(user_click_push_source):
    user_click_push_source["clicked"] = user_click_push_source["clicked"] + 42
    return user_click_push_source


@stream_feature_view(
    name="python_udf",
    source=FilteredSource(keyword_click_source),
    entities=[content_keyword],
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=30),
    tags={'release': 'production'},
    owner='achal@tecton.ai',
    description='The ad clicks for a user',
    mode='python',
    schema=output_schema,
    batch_trigger=BatchTriggerType.SCHEDULED,
    batch_compute=db_config,
)
def content_keyword_click_counts_python(user_click_push_source):
    user_click_push_source["clicked"] = user_click_push_source["clicked"] + 42
    second_record = user_click_push_source.copy()
    second_record["content_keyword"] = second_record["content_keyword"].upper()
    return [user_click_push_source, second_record]


@stream_feature_view(
    name="python_wafv",
    source=FilteredSource(keyword_click_source),
    entities=[content_keyword],
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    batch_schedule=timedelta(days=1),
    tags={'release': 'production'},
    owner='achal@tecton.ai',
    description='The ad clicks for a user',
    mode='python',
    aggregations=[
        Aggregation(column='clicked', function='sum', time_window=timedelta(hours=1)),
        Aggregation(column='clicked', function='sum', time_window=timedelta(hours=24)),
        Aggregation(column='clicked', function='sum', time_window=timedelta(hours=72)),
        Aggregation(column='clicked', function='sum', time_window=timedelta(days=7)),
    ],
    schema=output_schema,
    batch_compute=db_config,
)
def content_keyword_click_counts_wafv(user_click_push_source):
    user_click_push_source["clicked"] = user_click_push_source["clicked"] ** 2
    return user_click_push_source
