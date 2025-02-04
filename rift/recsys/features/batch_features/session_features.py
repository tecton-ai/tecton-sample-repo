from tecton import batch_feature_view, Aggregate
from tecton.aggregation_functions import approx_count_distinct
from tecton.types import Field, Int32

from recsys.entities import session
from recsys.data_sources.session_events import sessions_batch
from datetime import datetime, timedelta

@batch_feature_view(
    sources=[sessions_batch.unfiltered()],
    entities=[session],
    mode='pandas',
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 7, 31),
    batch_schedule=timedelta(days=1),
    aggregation_interval=timedelta(days=1),
    features=[
        Aggregate(input_column=Field('aid', Int32), function=approx_count_distinct(), time_window=timedelta(days=1)),
        Aggregate(input_column=Field('aid', Int32), function=approx_count_distinct(), time_window=timedelta(days=3)),
        Aggregate(input_column=Field('aid', Int32), function=approx_count_distinct(), time_window=timedelta(days=7))
    ],
    timestamp_field='ts',
    description='Number of articles in session over 1, 3, 7 days'
)
def session_approx_count_articles(sessions_data):
    return sessions_data[['session', 'aid', 'ts']]
