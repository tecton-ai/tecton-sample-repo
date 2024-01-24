from tecton import Aggregation, DeltaConfig
from tecton import stream_feature_view
from ads.entities import content_keyword
from tecton.types import Field, Int64, String, Timestamp, Float64
from datetime import timedelta
from datetime import datetime
from ads.data_sources.ad_sources import ingest_struct

schema = [
    Field(name="content_keyword", dtype=String),
    Field(name="timestamp", dtype=Timestamp),
    Field(name="clicked", dtype=Int64),
]


@stream_feature_view(
    name="ingest_struct_no_batch",
    source=ingest_struct,
    entities=[content_keyword],
    online=True,
    offline=True,
    offline_store=DeltaConfig(),
    feature_start_time=datetime(2023, 1, 1),
    aggregations=[
        Aggregation(column='clicked', function='sum', time_window=timedelta(hours=1)),
        Aggregation(column='clicked', function='sum', time_window=timedelta(days=1)),
        Aggregation(column='clicked', function='mean', time_window=timedelta(hours=1)),
        Aggregation(column='clicked', function='mean', time_window=timedelta(days=1)),
    ],
    tags={"release": "production"},
    owner="pooja@tecton.ai",
    description="Transaction amount statistics over a series of time windows",
    mode="python",
    schema=schema,
)
def ingest_struct_no_batch(source):
    output = {
        "content_keyword": source["content_keyword"],
        "timestamp": source["timestamp"],
        "clicked": source["simple_struct"].clicked
    }
    return output
