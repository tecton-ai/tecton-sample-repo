from tecton import batch_feature_view, FilteredSource
from entities import user
from data_sources.customers import customers
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[FilteredSource(customers)],
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2017,1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=3650),
    description='User date of birth, entered at signup.',
    timestamp_field='signup_timestamp'
)
def user_home_location(customers):
    return f"""
        SELECT
            signup_timestamp,
            user_id,
            lat,
            long
        FROM
            {customers}
    """
