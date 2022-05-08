from tecton import stream_feature_view, FilteredSource
from entities import user
from data_sources.transactions import transactions_stream
from datetime import datetime, timedelta


@stream_feature_view(
    sources=[FilteredSource(transactions_stream)],
    entities=[user],
    mode='pyspark',
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 5, 20),
    batch_schedule=timedelta(days=1),
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description='Last user transaction amount (stream calculated)'
)
def user_last_transaction_amount(transactions):
    from pyspark.sql import functions as f
    return transactions \
        .withColumnRenamed('nameorig', 'user_id') \
        .select('timestamp', 'user_id', 'amount')
