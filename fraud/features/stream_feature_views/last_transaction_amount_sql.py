from tecton.compat import stream_feature_view, Input, DatabricksClusterConfig, MonitoringConfig
from fraud.entities import user
from fraud.data_sources.transactions_stream import transactions_stream
from datetime import datetime

@stream_feature_view(
    inputs={'transactions': Input(transactions_stream)},
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2021, 5, 20),
    batch_schedule='1d',
    ttl='30days',
    monitoring=MonitoringConfig(alert_email="derek@tecton.ai", monitor_freshness=True),
    stream_cluster_config=DatabricksClusterConfig(number_of_workers=1),
    owner="derek@tecton.ai",
    family='fraud',
    description='Last user transaction amount (stream calculated)'
)
def last_transaction_amount_sql(transactions):
    return f'''
        SELECT
            timestamp,
            nameorig as user_id,
            amount
        FROM
            {transactions}
        '''
