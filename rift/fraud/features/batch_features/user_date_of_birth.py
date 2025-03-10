from tecton import batch_feature_view, Attribute
from tecton.types import String

from fraud.entities import user
from fraud.data_sources.fraud_users import fraud_users_batch
from datetime import datetime, timedelta
import pandas


@batch_feature_view(
    sources=[fraud_users_batch.unfiltered()],
    entities=[user],
    mode='pandas',
    online=False,
    offline=False,
    # Note the timestamp is the signup date, hence the old start_time.
    feature_start_time=datetime(2017,1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=3650),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='User date of birth, entered at signup.',
    features=[
        Attribute('user_date_of_birth', String)
    ],
    timestamp_field='timestamp'
)
def user_date_of_birth(fraud_users_batch):
    import pandas

    df = fraud_users_batch.copy()
    
    # Format date of birth to yyyy-MM-dd
    df['user_date_of_birth'] = pandas.to_datetime(df['dob']).dt.strftime('%Y-%m-%d')
    
    # Rename signup_timestamp to timestamp
    df = df.rename(columns={'signup_timestamp': 'timestamp'})
    
    # Select required columns
    return df[['user_id', 'user_date_of_birth', 'timestamp']]
