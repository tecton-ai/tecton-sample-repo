from tecton import batch_feature_view, Attribute
from tecton.types import Float64

from fraud.entities import user
from fraud.data_sources.fraud_users import fraud_users_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[fraud_users_batch.unfiltered()],
    entities=[user],
    mode='pandas',
    online=True,
    offline=True,
    # Note the timestamp is the signup date, hence the old start_time.
    feature_start_time=datetime(2017,1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=3650),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='User date of birth, entered at signup.',
    timestamp_field='signup_timestamp',
    features=[
        Attribute('lat', Float64),
        Attribute('long', Float64),
    ],

)
def user_home_location(fraud_users_batch):
    return fraud_users_batch[['signup_timestamp', 'user_id', 'lat', 'long']]
