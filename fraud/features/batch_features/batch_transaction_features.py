from tecton import batch_feature_view, FilteredSource
from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta
from configs import dataproc_config

@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=30),
    description='Last user transaction amount (batch calculated)',
    batch_compute=dataproc_config,
)
def last_transaction_amount(transactions_batch):
    return f'''
        SELECT
            timestamp,
            user_id,
            amt
        FROM
            {transactions_batch}
        '''
