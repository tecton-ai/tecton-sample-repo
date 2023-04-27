from datetime import timedelta, datetime
from tecton import FilteredSource, stream_feature_view, BatchTriggerType, DatabricksClusterConfig
from ads.entities import content_keyword
from ads.data_sources.ad_impressions import keyword_click_source
from tecton.types import Field, Int64, String, Timestamp

db_config = DatabricksClusterConfig(dbr_version="10.4.x-scala2.12")

def _get_ds_schema_for_fv():
    return [
        Field("content_keyword", String),
        Field("timestamp", Timestamp),
        Field("clicked", Int64),
        Field("clicked_plus_42", Int64),
    ]

@stream_feature_view(
    name="keyword_clicks_fv_with_pandas_filter",
    source=FilteredSource(keyword_click_source), # Pandas/Python transformation requires a PushSource.
    entities=[content_keyword],
    online=True,
    offline=True,
    mode="pandas", # Use mode pandas to execute pandas code and `python` to execute python code.
    feature_start_time=datetime(2023, 1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=30),
    tags={'release': 'production'},
    owner='pooja@tecton.ai',
    description='The ad clicks for a content keyword',
    schema=_get_ds_schema_for_fv(), # This stream feature view requires an input schema that defines what the data looks like after a transformation is applied.
    batch_trigger=BatchTriggerType.MANUAL, # Batch materialization must be manual.
    batch_compute=db_config, # Users will need Databricks 10.4 and up to enable PySpark
)
def content_keyword_click_counts_push_pandas(click_event_source):
    click_event_source = click_event_source[click_event_source["clicked"] > 0]
    click_event_source["clicked_plus_42"] = click_event_source["clicked"] + 42
    return click_event_source
