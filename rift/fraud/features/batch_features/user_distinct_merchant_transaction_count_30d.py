from tecton import batch_feature_view, Attribute
from tecton.types import Int64

from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta


# This feature view is an example of a "custom aggregation" using `incremental_backfills=True`. This allows users to
# compute aggregations over long windows of data using SQL aggregations.
#
# See this documentation for more info:
# https://docs.tecton.ai/latest/overviews/framework/feature_views/batch/incremental_backfills.html.
@batch_feature_view(
    sources=[transactions_batch],
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
    environment='rift_materialization_1-1-0b10'
)
def user_distinct_merchant_transaction_count_30d(transactions_batch):
    """
    Compute the count of distinct merchants per user over a 30-day window.
    
    Args:
        transactions_batch: DataFrame containing transaction data
        
    Returns:
        DataFrame with user_id, timestamp, and distinct merchant count
    """
    # Group by user_id and count distinct merchants
    df = transactions_batch.groupby('user_id').agg({
        'merchant': 'nunique'
    }).reset_index()
    
    # Rename the aggregated column
    df = df.rename(columns={'merchant': 'distinct_merchant_transaction_count_30d'})
    
    # Add timestamp column from the input DataFrame
    df['timestamp'] = transactions_batch['timestamp'].max()
    
    return df[['user_id', 'timestamp', 'distinct_merchant_transaction_count_30d']]
