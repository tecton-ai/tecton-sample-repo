from tecton import batch_feature_view, Attribute
from tecton.types import String

from fraud.entities import user
from fraud.data_sources.fraud_users import fraud_users_batch
from datetime import datetime, timedelta


@batch_feature_view(
    sources=[fraud_users_batch.unfiltered()],
    entities=[user],
    mode='pandas',
    online=False,
    offline=False,
    # Note the timestamp is the signup date, hence the old start_time.
    feature_start_time=datetime(2017, 1, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=3650),
    timestamp_field='signup_timestamp',
    prevent_destroy=False,  # Set to True to prevent accidental destructive changes or downtime.
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='User credit card issuer derived from the user credit card number.',
    features=[
        Attribute('credit_card_issuer', String)
    ]
)
def user_credit_card_issuer(fraud_users_batch):
    pandas_df = fraud_users_batch[['user_id', 'signup_timestamp', 'cc_num']].copy()
    
    # Convert cc_num to string and get first digit
    pandas_df['credit_card_issuer'] = pandas_df['cc_num'].astype(str).str[0].map({
        '4': 'Visa',
        '5': 'MasterCard',
        '6': 'Discover'
    }).fillna('other')
    
    return pandas_df[['user_id', 'signup_timestamp', 'credit_card_issuer']]
