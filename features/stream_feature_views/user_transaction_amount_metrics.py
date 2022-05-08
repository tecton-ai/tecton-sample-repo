from tecton import stream_feature_view, FilteredSource, Aggregation, DatabricksCompute
from entities import user
from data_sources.transactions import transactions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    sources=[FilteredSource(transactions_stream)],
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2020, 10, 10),
    batch_schedule=timedelta(days=1),
    stream_compute=DatabricksCompute(number_of_workers=1),
    alert_email='derek@tecton.ai',
    monitor_freshness=True,
    owner='kevin@tecton.ai',
    tags={'release': 'production'},
    description='Transaction amount statistics and total over a series of time windows, updated every 10 minutes.',
    aggregation_interval=timedelta(minutes=10),
    aggregations=[
        Aggregation(column='amount', function='mean', time_windows=['1h', '12h', '24h','72h']),
        Aggregation(column='amount', function='sum', time_windows=['1h', '12h', '24h','72h'])
    ]
)
def user_transaction_amount_metrics(transactions):
    return f'''
        SELECT
            nameorig as user_id,
            amount,
            timestamp
        FROM
            {transactions}
        '''
