from tecton import stream_feature_view, FilteredSource
from entities import user
from data_sources.transactions import transactions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    source=FilteredSource(transactions_stream),
    entities=[user],
    mode='pyspark',
    online=True,
    offline=False,
    feature_start_time=datetime(2021, 5, 20),
    schedule_interval=timedelta(days=1),
    ttl=timedelta(days=30),
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description='Last user transaction amount (stream calculated)'
)
def user_last_transaction_amount(transactions):
    from pyspark.sql import functions as f
    return transactions \
        .withColumnRenamed('nameorig', 'user_id') \
        .select('timestamp', 'user_id', 'amount')
