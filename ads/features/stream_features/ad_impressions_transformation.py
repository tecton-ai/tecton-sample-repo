
from tecton import stream_feature_view
from ads.entities import content_keyword
from tecton.types import Field, Int64, String, Timestamp, Float64
from datetime import timedelta
from datetime import datetime
from ads.data_sources.ad_sources import ad_ingest_no_batch


fv_output_schema = [
    Field(name="content_keyword", dtype=String),
    Field(name="timestamp", dtype=Timestamp),
    Field(name="clicked", dtype=Int64),
    Field(name="click_squared", dtype=Int64)
]

@stream_feature_view(
    source=ad_ingest_no_batch,
    entities=[content_keyword],
    mode='python',
    online=True,
    offline=True,
    schema=fv_output_schema,
    feature_start_time=datetime(2020, 1, 1),
    batch_schedule=timedelta(days=1)
)
def click_squared_fv(input):
    input["click_squared"] = input["clicked"] ** 2
    return input
