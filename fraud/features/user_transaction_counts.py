from tecton.feature_views import batch_window_aggregate_feature_view
from tecton.feature_views.feature_view import Input
from tecton import FeatureAggregation, NewDatabricksClusterConfig
from core.entities import user
from fraud.data_sources.transactions_batch import transactions_batch
from datetime import datetime


@batch_window_aggregate_feature_view(
    inputs={'transactions': Input(transactions_batch)},
    entities=[user],
    mode='spark_sql',
    aggregation_slide_period='1d',
    aggregations=[FeatureAggregation(column='transaction', function='count', time_windows=['24h','72h','168h', '960h'])],
    online=True,
    offline=True,
    feature_start_time=datetime(2020, 10, 10),
    family='fraud',
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    description='User transaction totals over a series of time windows, updated hourly.',
    batch_materialization=NewDatabricksClusterConfig(instance_type="r5.large", number_of_workers=3)
)
def user_transaction_counts(transactions):
    return f'''
        SELECT
            nameorig as user_id,
            1 as transaction,
            timestamp
        FROM
            {transactions}
        '''
