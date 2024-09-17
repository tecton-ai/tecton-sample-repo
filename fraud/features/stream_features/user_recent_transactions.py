from tecton import stream_feature_view, FilteredSource, Aggregate, AggregationLeadingEdge
from tecton.aggregation_functions import last_distinct
from tecton.types import Field, String

from fraud.entities import user
from fraud.data_sources.transactions import transactions_stream
from datetime import datetime, timedelta


# The following defines a sliding time window aggregation that collects the last N transaction amounts of a user
@stream_feature_view(
    source=transactions_stream,
    entities=[user],
    mode='spark_sql',
    aggregation_interval=timedelta(minutes=10),  # Defines how frequently feature values get updated in the online store
    batch_schedule=timedelta(days=1), # Defines how frequently batch jobs are scheduled to ingest into the offline store
    features=[
        Aggregate(input_column=Field('amt', String), function=last_distinct(10), time_window=timedelta(hours=1))
    ],
    timestamp_field='timestamp',
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    prevent_destroy=False,  # Set to True to prevent accidental destructive changes or downtime.
    tags={'release': 'production'},
    owner='demo-user@tecton.ai',
    description='Most recent 10 transaction amounts of a user',
    aggregation_leading_edge=AggregationLeadingEdge.LATEST_EVENT_TIME
)
def user_recent_transactions(transactions):
    return f'''
        SELECT
            user_id,
            cast(amt as string) as amt,
            timestamp
        FROM
            {transactions}
        '''
