from datetime import timedelta, datetime

from tecton import Aggregate, AggregationLeadingEdge
from tecton.types import Int64, Field
from tecton import StreamFeatureView
from ads.entities import user
from ads.data_sources.ad_impressions import user_click_push_source

# Windowed aggregations of user-keyed ad-click count events using the Tecton Stream Ingest API.
#
# See the documentation:
# https://docs.tecton.ai/using-the-ingestion-api/#creating-a-stream-feature-view-with-a-push-source
user_click_counts_push = StreamFeatureView(
    name="user_click_counts_wafv",
    source=user_click_push_source,
    entities=[user],
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    alert_email="demo-user@tecton.ai",
    timestamp_field="timestamp",
    features=[Aggregate(input_column=Field("clicked", Int64), function="count", time_window=timedelta(hours=1)), Aggregate(input_column=Field("clicked", Int64), function="count", time_window=timedelta(days=1)), Aggregate(input_column=Field("clicked", Int64), function="count", time_window=timedelta(days=3))],
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    aggregation_leading_edge=AggregationLeadingEdge.LATEST_EVENT_TIME,
    description='The count of ad clicks for a user'
)
