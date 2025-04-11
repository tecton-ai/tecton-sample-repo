from tecton import batch_feature_view, Aggregate
from tecton.aggregation_functions import approx_count_distinct
from tecton.types import Field, Int32, Int64

from recsys.entities import article
from recsys.data_sources.session_events import sessions_batch
from datetime import timedelta


@batch_feature_view(
    description="Unique sessions with article interactions",
    sources=[sessions_batch.unfiltered()],
    entities=[article],
    mode="pandas",
    timestamp_field="ts",
    aggregation_interval=timedelta(days=1),
    features=[
        Aggregate(input_column=Field("session", Int32), function=approx_count_distinct(), time_window=timedelta(days=30)),
    ],
)
def article_sessions(sessions_batch):
    df = sessions_batch[['session', 'aid', 'ts']]
    result = df.groupby('aid').agg({
        'session': lambda x: len(x.unique()),
        'ts': 'max'
    }).reset_index()
    return result

@batch_feature_view(
    description="Article interactions: aggregations of clicks, carts, orders on an article",
    sources=[sessions_batch.unfiltered()],
    entities=[article],
    mode="pandas",
    timestamp_field="ts",
    aggregation_interval=timedelta(days=1),
    features=[
        Aggregate(function=approx_count_distinct(), 
                  input_column=Field("interaction_approx_count_distinct_30d", Int64), 
                  time_window=timedelta(days=30),
                  description="Count of unique interactions on an article"
                  )
    ],
)
def article_interactions(sessions_batch):
    df = sessions_batch[['aid', 'ts', 'interaction']]
    df['aid'] = df['aid'].astype('int64')
    df = df.rename(columns={'interaction': 'interaction_approx_count_distinct_30d'})
    result = df.groupby('aid').agg({
        'interaction_approx_count_distinct_30d': lambda x: len(x.unique()),
        'ts': 'max'
    }).reset_index()
    return result
