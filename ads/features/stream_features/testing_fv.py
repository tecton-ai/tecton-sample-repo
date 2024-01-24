
from tecton import stream_feature_view
from ads.entities import fulfillment_option
from tecton.types import Field, Int64, String, Timestamp, Float64
from datetime import timedelta
from datetime import datetime
from ads.data_sources.ad_sources import fulfillment_option_source


fv_output_schema = [
    Field(name="fulfillment_option_id", dtype=Int64),
    Field(name="timestamp_key", dtype=Timestamp),
    Field(name="quote_id", dtype=Int64)
]

@stream_feature_view(
    source=fulfillment_option_source,
    entities=[fulfillment_option],
    mode='pandas',
    online=True,
    offline=True,
    schema=fv_output_schema,
    feature_start_time=datetime(2020, 1, 1),
    batch_schedule=timedelta(days=1)
)
def fulfillment_option_selected_fv(fulfillment_options):
    import pandas as pd
    selected_df = fulfillment_options[fulfillment_options['selected']==True]
    result_df = selected_df[['fulfillment_option_id', 'timestamp_key', 'quote_id']]
    return result_df
