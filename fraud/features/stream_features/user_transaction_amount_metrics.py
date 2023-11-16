from datetime import datetime, timedelta

from tecton import stream_feature_view, Aggregation, DeltaConfig
from tecton.types import Field, Int64, String, Timestamp, Float64

from fraud.data_sources.transactions import ingest_source
from fraud.entities import user

schema = [
            Field(name="user_id", dtype=String),
            Field(name="timestamp", dtype=Timestamp),
            Field(name="amt", dtype=Float64),
]

@stream_feature_view(
    name="user_transaction_metrics",
    source=ingest_source,
    entities=[user],
    online=True,
    offline=True,
    offline_store=DeltaConfig(),
    feature_start_time=datetime(2023, 1, 1),
    aggregations=[
        Aggregation(column='amt', function='sum', time_window=timedelta(hours=1)),
        Aggregation(column='amt', function='sum', time_window=timedelta(days=1)),
        Aggregation(column='amt', function='mean', time_window=timedelta(hours=1)),
        Aggregation(column='amt', function='mean', time_window=timedelta(days=1)),
    ],
    tags={"release": "production"},
    owner="pooja@tecton.ai",
    description="Transaction amount statistics over a series of time windows",
    mode="python",
    schema=schema,
)
def user_transaction_metrics(transactions):
    transactions["amt"] = transactions["amt"].fillna(0).astype("float64")
    return transactions
