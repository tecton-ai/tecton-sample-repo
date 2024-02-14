from datetime import timedelta, datetime
from tecton import stream_feature_view, Aggregation, Entity
from tecton.types import Field, Float64, String, Timestamp
from ads.entities import user
from ads.data_sources.ad_sources import ecommerce_source

# The output schema of the Feature View's transformation
schema = [
    Field('user_id', dtype=String),
    Field('timestamp', dtype=Timestamp),
    Field('price', dtype=Float64),
]


@stream_feature_view(
    source=ecommerce_source,
    entities=[user],
    online=True,
    offline=True,
    feature_start_time=datetime(2024, 1, 1),
    mode="python",
    batch_schedule=timedelta(days=1),
    schema=schema,
    aggregations=[
        Aggregation(column="price", function="count", time_window=timedelta(days=1)),
        Aggregation(column="price", function="count", time_window=timedelta(days=24)),
        Aggregation(column="price", function="count", time_window=timedelta(days=72)),
        Aggregation(column="price", function="mean", time_window=timedelta(days=1)),
        Aggregation(column="price", function="mean", time_window=timedelta(days=24)),
        Aggregation(column="price", function="mean", time_window=timedelta(days=72)),
    ],
    description="The average price of products in a user cart",
)
def average_product_price(event):
    return {
        "user_id": event["user_id"],
        "timestamp": event["timestamp"],
        "price": float(event["product_details"]["price"])
    }
