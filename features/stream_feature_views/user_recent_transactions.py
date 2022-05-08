from tecton import stream_feature_view, FilteredSource, Aggregation
from tecton.aggregation_functions import last_distinct
from entities import user
from data_sources.transactions import transactions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    sources=[FilteredSource(transactions_stream)],
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    owner='kevin@tecton.ai',
    tags={'release': 'production'},
    description='Most recent 10 transaction amounts of a user',
    batch_schedule=timedelta(days=1),
    aggregation_interval=timedelta(minutes=10),
    aggregations=[
        Aggregation(column='amount', function=last_distinct(10),  time_windows=['1h', '12h', '24h','72h'])
    ]
)
def user_recent_transactions(transactions):
    return f'''
        SELECT
            nameorig as user_id,
            cast(amount as string) as amount,
            timestamp
        FROM
            {transactions}
        '''
