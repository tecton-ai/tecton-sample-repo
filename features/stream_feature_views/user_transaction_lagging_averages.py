from tecton import stream_feature_view, FilteredSource, FeatureAggregation
from entities import user
from data_sources.transactions import transactions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    source=FilteredSource(transactions_stream),
    entities=[user],
    mode='spark_sql',
    aggregation_interval=timedelta(minutes=10),
    aggregations=[
        FeatureAggregation(
            column='amount',
            function='mean',
            time_windows=[timedelta(hours=2), timedelta(hours=1)])
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
