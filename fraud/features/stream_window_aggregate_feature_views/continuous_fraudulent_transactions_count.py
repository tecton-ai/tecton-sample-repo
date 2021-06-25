from tecton import stream_window_aggregate_feature_view, FeatureAggregation, Input, DatabricksClusterConfig
from fraud.entities import user
from fraud.data_sources.transactions_stream import transactions_stream
from datetime import datetime

# The following defines a continuous streaming feature
# It counts the number of fraudulent transactions per user over a 1min, 5min and 1h time window
# The expected freshness for these features is <1second
#
# Note: We use on-demand instances for continuous mode here to avoid spot
# instance termination failures which can increase feature freshness
@stream_window_aggregate_feature_view(
    inputs={'transactions': Input(transactions_stream)},
    entities=[user],
    mode='spark_sql',
    aggregation_slide_period='continuous',
    aggregations=[
        FeatureAggregation(column='counter', function='count', time_windows=['1min', '5min', '1h'])
    ],
    stream_cluster_config = DatabricksClusterConfig(
        instance_availability='on_demand',
    ),
    online=True,
    offline=True,
    feature_start_time=datetime(2020, 10, 10),
    family='fraud',
    tags={'release': 'production'},
    owner='kevin@tecton.ai',
    description='Number of fraudulent transactions'
)
def continuous_fraudulent_transactions_count(transactions):
    return f'''
        SELECT
            nameorig as user_id,
            1 as counter,
            timestamp
        FROM
            {transactions}
        WHERE isFraud != 0
        '''

