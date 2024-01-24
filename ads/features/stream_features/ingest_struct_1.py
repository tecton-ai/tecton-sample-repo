from tecton import Aggregation, DeltaConfig
from tecton import stream_feature_view
from ads.entities import content_keyword
from tecton.types import Field, Int64, String, Timestamp, Float64, Struct
from datetime import timedelta
from datetime import datetime
from ads.data_sources.ad_sources import ingest_struct

simple_struct = Struct(
    [
        Field("clicked", Int64),
        Field("clicked_str", String)
    ]
)

schema = [
    Field(name="content_keyword", dtype=String),
    Field(name="timestamp", dtype=Timestamp),
    Field("clicked_struct", simple_struct)
]


@stream_feature_view(
    name="ingest_struct_feature",
    source=ingest_struct,
    entities=[content_keyword],
    online=True,
    offline=True,
    offline_store=DeltaConfig(),
    feature_start_time=datetime(2023, 1, 1),
    tags={"release": "production"},
    owner="pooja@tecton.ai",
    description="Transaction amount statistics over a series of time windows",
    mode="python",
    schema=schema,
)
def ingest_struct_feature(source):
    output = {
        "content_keyword": source["content_keyword"],
        "timestamp": source["timestamp"],
        "clicked_struct": source["simple_struct"]
    }
    return output
