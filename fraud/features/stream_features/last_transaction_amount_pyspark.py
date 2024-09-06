from tecton import FilteredSource
from tecton import stream_feature_view, AggregationLeadingEdge, Attribute
from tecton.types import Field, Float64
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
    timestamp_field="timestamp",
    aggregation_leading_edge=AggregationLeadingEdge.LATEST_EVENT_TIME,
    features=[
        Attribute("amt", dtype=Float64)
    ]
)
def last_transaction_amount_pyspark(transactions):
    from pyspark.sql import functions as f
    return transactions.select('timestamp', 'user_id', 'amt')
