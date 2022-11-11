from tecton import stream_feature_view, FilteredSource, DatabricksClusterConfig
from fraud.entities import user
from fraud.data_sources.transactions import transactions_stream
from datetime import datetime, timedelta

@stream_feature_view(
    source=transactions_stream,
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=30),
    description='Last user transaction amount (stream calculated)',
    stream_compute=DatabricksClusterConfig(number_of_workers=0, instance_type='m5.large'),
)
def last_transaction_amount_sql(transactions):
    return f'''
        SELECT
            timestamp,
            user_id,
            amt
        FROM
            {transactions}
        '''
