from tecton import Aggregation
from tecton import StreamFeatureView
from ads.entities import content_keyword
from tecton.aggregation_functions import last
from datetime import timedelta
from datetime import datetime
from ads.data_sources.ad_sources import ad_ingest_no_batch

agg_col1 = "clicked"

impression_wafv_no_batch = StreamFeatureView(
    name="impressions_wafv_no_batch",
    source=ad_ingest_no_batch,
    entities=[content_keyword],
    online=True,
    offline=True,
    batch_schedule=timedelta(days=1),
    aggregations=[
        Aggregation(
            column=agg_col1,
            function="count",
            time_window=timedelta(days=1),
        ),
        Aggregation(
            column=agg_col1,
            function="mean",
            time_window=timedelta(days=1),
        ),
        Aggregation(
            column=agg_col1,
            function="sum",
            time_window=timedelta(days=1),
        ),
        Aggregation(
            column=agg_col1,
            function="max",
            time_window=timedelta(days=1),
        ),
        Aggregation(
            column=agg_col1,
            function="min",
            time_window=timedelta(days=1),
        ),
        Aggregation(
            column="clicked_str",
            function=last(3),
            time_window=timedelta(days=1),
        ),
        Aggregation(
            column=agg_col1,
            function="stddev",
            time_window=timedelta(days=1),
        ),
    ],
    feature_start_time=datetime(2023, 1, 1),
    owner="pooja@tecton.ai",
    description="The impressions of an ad for a user",
)
