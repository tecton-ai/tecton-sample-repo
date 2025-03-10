from tecton import batch_feature_view, Attribute
from tecton.types import Int32
import pandas

from fraud.entities import user
from fraud.data_sources.fraud_users import fraud_users_batch
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


# For every transaction, the following FeatureView precomputes a feature that indicates
# whether a user was an adult as of the time of the transaction
@batch_feature_view(
    sources=[transactions_batch, fraud_users_batch.unfiltered()],
    entities=[user],
    mode='pandas',
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=100),
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='Whether the user performing the transaction is over 18 years old.',
    features=[
        Attribute('user_is_adult', Int32)
    ],
    timestamp_field='timestamp'
)
def transaction_user_is_adult(transactions_batch, fraud_users_batch):
    import pandas

    # Merge transactions with user data
    df = transactions_batch.merge(
        fraud_users_batch,
        on='user_id',
        how='inner'
    )
    
    # Calculate age in days and create user_is_adult column
    df['user_is_adult'] = ((df['timestamp'] - pandas.to_datetime(df['dob'])).dt.days > (18*365)).astype(int)
    
    # Select and order final columns
    return df[['timestamp', 'user_id', 'user_is_adult']]
