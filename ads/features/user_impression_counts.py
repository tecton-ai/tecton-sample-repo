from tecton.feature_views import stream_window_aggregate_feature_view
from tecton.feature_views.feature_view import Input
from tecton import FeatureAggregation
from core.entities import user
from ads.entities import ad
from ads.data_sources.ad_impressions_stream import ad_impressions_stream
from datetime import datetime


@stream_window_aggregate_feature_view(
    inputs={"ad_impressions": Input(ad_impressions_stream)},
    entities=[user],
    mode="spark_sql",
    aggregation_slide_period="1h",
    aggregations=[FeatureAggregation(column="impression", function="count", time_windows=["1h", "12h", "24h","72h","168h"])],
    online=True,
    offline=True,
    batch_schedule="1d",
    feature_start_time=datetime(2021, 1, 1),
    family='ads',
    tags={'release': 'production'},
    owner="matt@tecton.ai",
    description="The count of ad impressions for a user"
)
def user_impression_counts(ad_impressions):
    return f"""
        select
            user_uuid as user_id,
            1 as impression,
            timestamp
        from
            {ad_impressions}
        """
