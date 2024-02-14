from datetime import timedelta, datetime
from tecton import stream_feature_view, Aggregation, Entity
from tecton.types import Field, Float64, String, Timestamp
from ads.entities import user
from ads.data_sources.ad_sources import ecommerce_source

# The output schema of the Feature View's transformation
schema = [
    Field('user_id', dtype=String),
    Field('timestamp', dtype=Timestamp),
    Field('preference_score', dtype=Float64),
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
    description="User preference score for the electronics category",
)
def electronics_preference_feature(event):
    return {
        "user_id": event["user_id"],
        "timestamp": event["timestamp"],
        "preference_score": event["user_preferences"].get("electronics", 0)
    }
