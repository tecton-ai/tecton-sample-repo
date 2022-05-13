from tecton import stream_feature_view, FilteredSource, FeatureAggregation, AggregationMode
from entities import user
from data_sources.transactions import transactions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    source=FilteredSource(transactions_stream),
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 6, 1),
    owner='kevin@tecton.ai',
    tags={'release': 'production'},
    description='Continuous count of non-fraudulent transactions',
    aggregation_mode=AggregationMode.CONTINUOUS,
    aggregations=[
        FeatureAggregation(column='counter', function='count', time_windows=[timedelta(minutes=1), timedelta(minutes=5), timedelta(hours=1)])
    ]
)
def user_continuous_non_fraudulent_transactions_count(transactions):
    return f'''
        SELECT
            nameorig as user_id,
            1 as counter,
            timestamp
        FROM
            {transactions}
        WHERE isFraud = 0
        '''
