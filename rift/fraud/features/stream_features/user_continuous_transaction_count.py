from tecton import stream_feature_view, StreamProcessingMode, Aggregate, AggregationLeadingEdge
from tecton.types import Int32, Field

from fraud.entities import user
from fraud.data_sources.transactions import transactions_stream
from datetime import datetime, timedelta

# The following defines a continuous streaming feature
# It counts the number of non-fraudulent transactions per user over a 1min, 5min and 1h time window
# The expected freshness for these features is <1second
@stream_feature_view(
    source=transactions_stream,
    entities=[user],
    mode='pandas',
    stream_processing_mode=StreamProcessingMode.CONTINUOUS,
    features=[
        Aggregate(input_column=Field('transaction', Int32), function='count', time_window=timedelta(minutes=1), name='transaction_1min'),
        Aggregate(input_column=Field('transaction', Int32), function='count', time_window=timedelta(minutes=30), name='transaction_30min'),
        Aggregate(input_column=Field('transaction', Int32), function='count', time_window=timedelta(hours=1), name='transaction_1h')
    ],
    online=False,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    prevent_destroy=False,  # Set to True to prevent accidental destructive changes or downtime.
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='Number of transactions a user has made recently',
    timestamp_field='timestamp',
    aggregation_leading_edge=AggregationLeadingEdge.LATEST_EVENT_TIME,
    environment='tecton-core-1.1.0'
)
def user_continuous_transaction_count(transactions):
    # Create a copy of the input DataFrame with only the required columns
    pandas_df = transactions[['user_id', 'timestamp']].copy()
    
    # Add a column for counting transactions
    pandas_df['transaction'] = 1
    
    return pandas_df
