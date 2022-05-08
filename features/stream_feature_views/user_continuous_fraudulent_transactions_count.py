from tecton import stream_feature_view, FilteredSource, Aggregation
from entities import user
from data_sources.transactions import transactions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    sources=[FilteredSource(transactions_stream)],
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 6, 1),
    owner='kevin@tecton.ai',
    tags={'release': 'production'},
    description='Continuous count of fraudulent transactions',
    aggregation_interval=timedelta(minutes=0),
    aggregations=[
        Aggregation(column='counter', function='count', time_windows=['1min', '5min', '1h'])
    ],
)
def user_continuous_fraudulent_transactions_count(transactions):
    return f'''
        SELECT
            nameorig as user_id,
            1 as counter,
            timestamp
        FROM
            {transactions}
        WHERE isFraud != 0
        '''
