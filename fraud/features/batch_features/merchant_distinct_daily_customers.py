from tecton import batch_feature_view, FilteredSource, materialization_context, Aggregation
from fraud.entities import merchant
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta

# Example feature view showing off a "tumbling window" aggregation, i.e. a query that groups rows into "windows"
# in a non-overlapping manner. Tumbling windows can be used in batch feature views to efficiently compute features
# aggregated over the `batch_schedule`.
#
# In this instance, this query produces the number of distinct customers that a merchant has in a given day. That data
# is then aggregated to produce weekly and monthly rolling averages of the merchant daily distinct user count.
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
        WITH transactions_with_window AS (
            SELECT
                *,
                -- Associate each row in `transactions_batch` with a materialization period, in this case the end of the
                -- UTC day because `batch_schedule` is 1 day.
                --
                -- On incremental materialization runs (i.e. the daily jobs), all of the data will be from the same
                -- materialization period, and the second GROUP BY dimension, `window_end`, will be a no-op.
                --
                -- When backfilling, this query will may process months of data. Grouping by `window_end`
                -- ensures that the backfill query will produce feature data for every day in the backfill range.
                WINDOW(timestamp, '{context.batch_schedule.total_seconds()} seconds').end as window_end
            FROM {transactions_batch}
        )
        SELECT
            merchant,
            -- The timestamp for the "daily aggregate" needs to be a timestamp from that day. The last microsecond
            -- of the day guarantees that the aggregate does not include any future events.
            window_end - INTERVAL 1 MICROSECOND as timestamp,
            COUNT(DISTINCT user_id) as num_daily_distinct_customers
        FROM transactions_with_window
        GROUP BY
            merchant, 
            window_end
        '''
