from tecton import stream_feature_view, FilteredSource, Aggregation, AggregationMode, DatabricksClusterConfig
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
    aggregation_mode=AggregationMode.CONTINUOUS,
    aggregations=[
        Aggregation(column='transaction', function='count', time_window=timedelta(minutes=1)),
        Aggregation(column='transaction', function='count', time_window=timedelta(minutes=30)),
        Aggregation(column='transaction', function='count', time_window=timedelta(hours=1))
    ],
    stream_compute=DatabricksClusterConfig(number_of_workers=0, instance_type='m5.large'),
    online=False,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='david@tecton.ai',
    description='Number of transactions a user has made recently'
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
