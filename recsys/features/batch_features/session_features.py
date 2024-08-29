from tecton.v09_compat import batch_feature_view, Aggregation
from tecton.aggregation_functions import approx_count_distinct
from recsys.entities import session
from recsys.data_sources.session_events import sessions_batch
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[sessions_batch],
    entities=[session],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 7, 31),
    batch_schedule=timedelta(days=1),
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(function=approx_count_distinct(), column="aid", time_window=timedelta(days=1)),
        Aggregation(function=approx_count_distinct(), column="aid", time_window=timedelta(days=3)),
        Aggregation(function=approx_count_distinct(), column="aid", time_window=timedelta(days=7))
    ],
    description='Number of articles in session over 1, 3, 7 days'
)
def session_approx_count_articles(sessions_data):
    return f"""
        SELECT session, aid, ts
        FROM {sessions_data}
        """
