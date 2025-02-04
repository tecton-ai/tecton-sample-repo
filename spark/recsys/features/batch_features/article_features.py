from tecton import batch_feature_view, Aggregate
from tecton.aggregation_functions import approx_count_distinct
from tecton.types import Field, Int32

from recsys.entities import article
from recsys.data_sources.session_events import sessions_batch
from datetime import timedelta


@batch_feature_view(
    description="Unique sessions with article interactions",
    sources=[sessions_batch.unfiltered()],
    entities=[article],
    mode="spark_sql",
    timestamp_field="ts",
    aggregation_interval=timedelta(days=1),
    features=[
        Aggregate(input_column=Field("session", Int32), function=approx_count_distinct(), time_window=timedelta(days=30)),
    ],
)
def article_sessions(sessions_batch):
    return f"""
        SELECT session, aid, ts
        FROM {sessions_batch}
        """

# Secondary Key Aggregate example - available in 0.8
# @batch_feature_view(
#     description="Article interactions: clicks, carts, orders on an article",
#     sources=[sessions_batch],
#     aggregation_secondary_key="type",
#     entities=[article],
#     mode="spark_sql",
#     timestamp_field="ts",
#     aggregation_interval=timedelta(days=1),
#     aggregations=[
#         Aggregation(function="count", column="interaction", time_window=timedelta(days=30)),
#     ],
# )
# def article_interactions(sessions_batch):
#     return f"""
#         SELECT aid, ts, type, 1 as interaction
#         FROM {sessions_batch}
#         """