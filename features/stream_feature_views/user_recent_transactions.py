from tecton import stream_feature_view, FilteredSource, FeatureAggregation
from tecton.aggregation_functions import last_distinct
from entities import user
from data_sources.transactions import transactions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    source=FilteredSource(transactions_stream),
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    owner='kevin@tecton.ai',
    tags={'release': 'production'},
    description='Most recent 10 transaction amounts of a user',
    schedule_interval=timedelta(days=1),
    aggregation_interval=timedelta(minutes=10),
    aggregations=[
        FeatureAggregation(column='amount', function=last_distinct(10),  time_windows=[timedelta(hours=1), timedelta(hours=12), timedelta(hours=24), timedelta(hours=72)])
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
