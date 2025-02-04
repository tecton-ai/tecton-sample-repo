from tecton import batch_feature_view, Attribute
from tecton.types import Float64

from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[transactions_batch],
    entities=[user],
    mode='pandas',
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=30),
    description='Last user transaction amount (batch calculated)',
    features=[
        Attribute('amt', Float64)
    ],
    timestamp_field='timestamp'
)
def last_transaction_amount(transactions_batch):
    return transactions_batch[['timestamp', 'user_id', 'amt']]
