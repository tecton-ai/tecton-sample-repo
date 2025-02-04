from tecton import stream_feature_view, Attribute, AggregationLeadingEdge
from tecton.types import Float64

from fraud.entities import user
from fraud.data_sources.transactions import transactions_stream
from datetime import datetime, timedelta

@stream_feature_view(
    source=transactions_stream.unfiltered(),
    entities=[user],
    mode='pandas',
    online=False,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=30),
    description='Last user transaction amount (stream calculated)',
    features=[
        Attribute('amt', Float64)
    ],
    timestamp_field='timestamp',
    aggregation_leading_edge=AggregationLeadingEdge.LATEST_EVENT_TIME
)
def last_transaction_amount_pyspark(transactions):
    return transactions[['timestamp', 'user_id', 'amt']]
