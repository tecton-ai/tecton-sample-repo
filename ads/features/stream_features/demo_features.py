from datetime import datetime, timedelta
from tecton import Entity, PushSource, stream_feature_view, Aggregation, BatchTriggerType, FilteredSource, HiveConfig, FeatureService, DatabricksClusterConfig, DatetimePartitionColumn
from tecton.types import Field, String, Int64, Timestamp, Array, Float64
from tecton.aggregation_functions import last

input_schema = [
    Field(name="user_id", dtype=String),
    Field(name="timestamp", dtype=Timestamp),
    Field(name="user_embeddings", dtype=Array(Float64)),
]


impressions_event_source = PushSource(
    name="user_embedding_source",
    schema=input_schema,
    description="Sample Push Source for ad impression events",
    owner="achal@tecton.ai",
    tags={"release": "production"},
)

user_id = Entity(name="User", join_keys=["user_id"])

output_schema = input_schema + [
    Field(name="user_embeddings_mean", dtype=Float64),
]


@stream_feature_view(
    source=impressions_event_source,
    entities=[user_id],
    online=True,
    offline=True,
    mode="pandas",
    schema=output_schema,
    feature_start_time=datetime(2023, 5, 1),
    ttl=timedelta(days=30),
    batch_trigger=BatchTriggerType.NO_BATCH_MATERIALIZATION,
    owner='derek@tecton.ai',
    alert_email='derek@tecton.ai',
    monitor_freshness=False,
)
def sidebar_ad_impressions(user_embedding_source):
    user_embedding_source.info()
    user_embedding_source["user_embeddings_mean"] = user_embedding_source["user_embeddings"].apply(lambda x: x.mean())
    return user_embedding_source


fs = FeatureService(
    name="user-fs",
    features=[sidebar_ad_impressions]
)