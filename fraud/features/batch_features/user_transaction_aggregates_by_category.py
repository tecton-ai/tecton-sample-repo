from tecton import batch_feature_view, Aggregation, FilteredSource
from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta
from functools import reduce

CATEGORIES = [
    'gas_transport',
    'home',
    'kids_pets',
    'shopping_pos',
    'grocery_pos',
    'shopping_net',
    'food_dining',
    'entertainment',
    'personal_care',
    'health_fitness',
    'misc_pos',
    'misc_net',
    'travel',
    'grocery_net',
]

aggregations = [
    [
        Aggregation(column=f'{category}_amt', function='sum', time_window=timedelta(days=30)),
        Aggregation(column=f'{category}_amt', function='mean', time_window=timedelta(days=30))
    ]
    for category in CATEGORIES
]

# Flatten the `aggregations` list of lists.
aggregations = reduce(lambda l, r: l + r, aggregations)

# This feature view produces aggregate metrics for each purchase category in a user's transaction history, e.g. how much
# has the user spent on "health_fitness" in the past 30 days. This feature view creates two aggregate features for each
# of the 14 categories for a total of 28 features.
@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[user],
    mode='pyspark',
    aggregation_interval=timedelta(days=1),
    aggregations=aggregations,
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 7, 1),
    description='User transaction aggregate metrics split by purchase category.'
)
def user_transaction_aggregates_by_category(transactions_df):
    from pyspark.sql.functions import col, when

    df = transactions_df

    for cat in CATEGORIES:
        df = df.withColumn(f'{cat}_amt', when(col('category') == cat, col('amt')).otherwise(None))

    final_columns = ['user_id', 'timestamp'] + [f'{cat}_amt' for cat in CATEGORIES]
    df = df.select(*final_columns)
    return df
