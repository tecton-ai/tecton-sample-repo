from tecton.v09_compat import stream_feature_view, FilteredSource
from fraud.entities import user
from fraud.data_sources.transactions import transactions_stream
from datetime import datetime, timedelta

@stream_feature_view(
    source=transactions_stream,
    entities=[user],
    mode='pyspark',
    online=False,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=30),
    description='Last user transaction amount (stream calculated)',
)
def last_transaction_amount_pyspark(transactions):
    from pyspark.sql import functions as f
    return transactions.select('timestamp', 'user_id', 'amt')
