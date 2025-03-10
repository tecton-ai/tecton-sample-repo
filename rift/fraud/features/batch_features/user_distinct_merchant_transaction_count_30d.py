from tecton import batch_feature_view, Attribute, TectonTimeConstant
from tecton.types import Int64
import pandas

from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


# This feature view is an example of a "custom aggregation" using `incremental_backfills=True`. This allows users to
# compute aggregations over long windows of data using SQL aggregations.
#
# See this documentation for more info:
# https://docs.tecton.ai/latest/overviews/framework/feature_views/batch/incremental_backfills.html.
@batch_feature_view(
    sources=[transactions_batch.select_range(start_time=TectonTimeConstant.MATERIALIZATION_START_TIME - timedelta(days=29), end_time=TectonTimeConstant.MATERIALIZATION_END_TIME)],
    entities=[user],
    mode='pandas',
    online=True,
    offline=True,
    feature_start_time=datetime(2022, 4, 1),
    incremental_backfills=True,
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=2),
    owner='demo-user@tecton.ai',
    tags={'release': 'production'},
    description='How many transactions the user has made to distinct merchants in the last 30 days.',
    features=[
        Attribute('distinct_merchant_transaction_count_30d', Int64)
    ],
    timestamp_field='timestamp',
)
def user_distinct_merchant_transaction_count_30d(transactions_batch, context):
    import pandas

    # Group by user_id and count distinct merchants
    df = transactions_batch.groupby('user_id').agg({
        'merchant': 'nunique'
    }).reset_index()
    
    # Rename the aggregated column
    df = df.rename(columns={'merchant': 'distinct_merchant_transaction_count_30d'})
    
    # Add timestamp column (end_time - 1 microsecond)
    df['timestamp'] = pandas.Timestamp(context.end_time) - pandas.Timedelta(microseconds=1)
    
    return df[['user_id', 'timestamp', 'distinct_merchant_transaction_count_30d']]
