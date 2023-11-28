from tecton import stream_feature_view, Aggregation, FilteredSource
from ads.entities import ad, user
from ads.data_sources.ad_impressions import ad_impressions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    source=FilteredSource(ad_impressions_stream),
    entities=[user, ad],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=1),
    aggregations=[
        Aggregation(column='impression', function='count', time_window=timedelta(hours=1)),
        Aggregation(column='impression', function='count', time_window=timedelta(hours=24)),
        Aggregation(column='impression', function='count', time_window=timedelta(hours=72)),
    ],
    online=False,
    offline=False,
    batch_schedule=timedelta(days=1),
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='The count of impressions between a given user and a given ad'
)
def user_ad_impression_counts(ad_impressions):
    return f"""
        SELECT
            user_uuid as user_id,
            ad_id,
            1 as impression,
            timestamp
        FROM
            {ad_impressions}
        """
