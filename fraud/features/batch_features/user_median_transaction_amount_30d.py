from tecton import batch_feature_view, Aggregation, FilteredSource
from tecton.aggregation_functions import approx_percentile
from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta
from configs import dataproc_config

@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[user],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='amt', function=approx_percentile(percentile=0.5), time_window=timedelta(days=30)),
    ],
    description='Median transaction amount for a user over the last 30 days',
    batch_compute=dataproc_config,
)
def user_median_transaction_amount_30d(transactions_batch):
    return f'''
        SELECT
            timestamp,
            user_id,
            amt
        FROM
            {transactions_batch}
        '''
