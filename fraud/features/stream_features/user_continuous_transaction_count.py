from tecton import stream_feature_view, FilteredSource, Aggregation, StreamProcessingMode, Attribute, Aggregate
from tecton.types import Float64, Int32, Field

from fraud.entities import user
from fraud.data_sources.transactions import transactions_stream
from datetime import datetime, timedelta

# The following defines a continuous streaming feature
# It counts the number of non-fraudulent transactions per user over a 1min, 5min and 1h time window
# The expected freshness for these features is <1second
@stream_feature_view(
    source=FilteredSource(transactions_stream),
    entities=[user],
    mode='spark_sql',
    stream_processing_mode=StreamProcessingMode.CONTINUOUS,
    features=[
        Aggregate(input_column=Field('transaction', Int32), function='count', time_window=timedelta(minutes=1)),
        Aggregate(input_column=Field('transaction', Int32), function='count', time_window=timedelta(minutes=30)),
        Aggregate(input_column=Field('transaction', Int32), function='count', time_window=timedelta(hours=1))
    ],
    online=False,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    prevent_destroy=False,  # Set to True to prevent accidental destructive changes or downtime.
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='Number of transactions a user has made recently',
    timestamp_field='timestamp'
)
def user_continuous_transaction_count(transactions):
    return f'''
        SELECT
            user_id,
            1 as transaction,
            timestamp
        FROM
            {transactions}
        '''
