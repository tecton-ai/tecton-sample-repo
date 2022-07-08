from tecton.compat import stream_feature_view, Input
from fraud.entities import user
from fraud.data_sources.transactions_stream import transactions_stream
from datetime import datetime

@stream_feature_view(
    inputs={'transactions': Input(transactions_stream)},
    entities=[user],
    mode='pyspark',
    online=False,
    offline=False,
    feature_start_time=datetime(2021, 5, 20),
    batch_schedule='1d',
    ttl='30days',
    family='fraud',
    description='Last user transaction amount (stream calculated)'
)
def last_transaction_amount_pyspark(transactions):
    from pyspark.sql import functions as f
    return transactions \
        .withColumnRenamed('nameorig', 'user_id') \
        .select('timestamp', 'user_id', 'amount')

