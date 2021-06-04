from datetime import datetime
from tecton import FeatureAggregation, batch_window_aggregate_feature_view, Input

from ads.data_sources.ad_impressions_batch import ad_impressions_batch
from ads.data_sources.ad_users_batch import ad_users_batch
from ads.entities import ad

@batch_window_aggregate_feature_view(
    # Declaring the two inputs we will join for our features
    inputs={
        "ad_impressions": Input(ad_impressions_batch),
        "users": Input(ad_users_batch)
    },
    entities=[ad],
    mode="spark_sql",
    online=True,
    offline=True,
    feature_start_time=datetime(2020, 5, 1),
    batch_schedule='1day',
    aggregation_slide_period='12h',
    aggregations=[
        FeatureAggregation(column='days_since_signup', function='mean', time_windows=['12h', '24h', '72h']),
        FeatureAggregation(column='days_since_signup', function='max', time_windows=['12h', '24h', '72h'])
    ],
    description="Features based on time since user account created"
)
def ad_impression_user_account_age(ad_impressions, users):
    return f"""
        SELECT
            {ad_impressions}.ad_id,
            {ad_impressions}.timestamp,
            DATEDIFF({ad_impressions}.timestamp, {users}.signup_date) AS days_since_signup
        FROM {ad_impressions}
        JOIN {users} ON {ad_impressions}.user_uuid = {users}.user_uuid
    """
