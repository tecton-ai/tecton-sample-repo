from tecton import stream_feature_view, FilteredSource, Aggregation
from entities import user
from data_sources.transactions import transactions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    sources=[FilteredSource(transactions_stream)],
    entities=[user],
    mode='spark_sql',
    aggregation_interval=timedelta(minutes=10),
    aggregations=[
        Aggregation(
            column='amount',
            function='mean',
            time_windows=[Window(timedelta(hours=2), timedelta(hours=1)])
    ]
)
def user_transaction_lagging_averages(transactions):
    return f'''
        SELECT
            nameorig as user_id,
            amount,
            timestamp
        FROM
            {transactions}
        '''
