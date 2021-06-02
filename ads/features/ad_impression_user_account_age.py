from datetime import datetime
from tecton import FeatureAggregation, batch_window_aggregate_feature_view, Input

from ads.data_sources.ad_impressions_batch import ad_impressions_batch
from ads.data_sources.ad_users_batch import ad_users_batch
from ads.entities import ad

# This feature view joins together user and ad impression tables
# to calculate features based on time since sign up.
@batch_window_aggregate_feature_view(
    description="Features based on time since user account created",
    mode="spark_sql",
    entities=[ad],
    online=True,
    offline=True,
    feature_start_time=datetime(2020, 5, 1),
    batch_schedule='1day',
    # Declaring the two inputs we will join in the transformation
    inputs={"ad_impressions": Input(ad_impressions_batch),
            "users": Input(ad_users_batch)},
    aggregation_slide_period='12h',
    aggregations=[
        FeatureAggregation(column='days_since_signup', function='mean', time_windows=['12h', '24h', '72h']),
        FeatureAggregation(column='days_since_signup', function='max', time_windows=['12h', '24h', '72h'])
    ],
    family='ads',
    owner='derek@tecton.ai',
    tags={'release': 'production'}
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
