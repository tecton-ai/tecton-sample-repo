from tecton import batch_feature_view, FilteredSource, materialization_context, Aggregation
from fraud.entities import merchant
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta

# Example feature view showing off a "tumbling window" aggregation. In this instance, this query produces the number of
# distinct customers that a merchant has in a given day. That data is then aggregated to produce weekly and monthly
# rolling averages of the merchant daily distinct user count.
@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[merchant],
    mode='spark_sql',
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 5, 1),
    batch_schedule=timedelta(days=1),
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='num_daily_distinct_customers', function='mean', time_window=timedelta(days=7)),
        Aggregation(column='num_daily_distinct_customers', function='mean', time_window=timedelta(days=28))
    ],
    description='Features about the user\'s last transaction of the day.'
)
def merchant_distinct_daily_customers(transactions_batch, context=materialization_context()):
    return f'''
        SELECT
            merchant,
            MAX(timestamp) as timestamp,
            COUNT(DISTINCT user_id) as num_daily_distinct_customers
        FROM {transactions_batch}
        GROUP BY
            -- Group by merchant and the nearest materialization period, in this case the nearest UTC day because 
            -- `batch_schedule` is 1 day.
            --
            -- On incremental materialization runs (i.e. the daily jobs), all of the data will be from the same
            -- materialization period, and this second GROUP BY dimension will be a no-op.
            --
            -- When backfilling, this query will may process months of data. Grouping by the materialization period
            -- ensures that the backfill query will produce feature data for every day in the backfill range.
            merchant, 
            WINDOW(timestamp, '{context.batch_schedule.total_seconds()} seconds')
        '''
