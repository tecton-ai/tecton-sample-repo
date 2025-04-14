from tecton import batch_feature_view, Aggregate
from tecton.aggregation_functions import approx_count_distinct
from tecton.types import Field, String

from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


# This feature view is a simpler way to implement the features in user_distinct_merchant_transaction_count_30d.
# Instead of using a "custom aggregation" with `incremental_backfills=True`, it uses Tecton's built-in `approx_count_distinct` aggregation.
@batch_feature_view(
    sources=[transactions_batch],
    entities=[user],
    mode='pandas',
    online=True,
    offline=True,
    feature_start_time=datetime(2022, 4, 1),
    aggregation_interval=timedelta(days=1),
    features=[
        Aggregate(input_column=Field('merchant', String), function=approx_count_distinct(), time_window=timedelta(days=30))
    ],
    tags={'release': 'production'},
    description='How many transactions the user has made to distinct merchants in the last 30 days.',
    timestamp_field='timestamp',
    environment='tecton-core-1.1.0'
)
def user_approx_distinct_merchant_transaction_count_30d(transactions_batch):
    return transactions_batch[['user_id', 'timestamp', 'merchant']]
