from tecton.v09_compat import FilteredSource
from tecton import batch_feature_view, Aggregate
from tecton.types import Field, Float64
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

# This feature view produces aggregate metrics for each purchase category in a user's transaction history, e.g. how much
# has the user spent on "health_fitness" in the past 30 days. This feature view creates two aggregate features for each
# of the 14 categories for a total of 28 features.
#
# An equivalent feature view can be implemented in spark SQL using the PIVOT clause.
@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[user],
    mode='pyspark',
    aggregation_interval=timedelta(days=1),
    online=False,
    offline=False,
    feature_start_time=datetime(2022, 7, 1),
    description='User transaction aggregate metrics split by purchase category.',
    timestamp_field='timestamp',
    features=[
        Aggregate(input_column=Field("gas_transport_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("gas_transport_amt", Float64), function="mean", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("home_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("home_amt", Float64), function="mean", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("kids_pets_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("kids_pets_amt", Float64), function="mean", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("shopping_pos_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("shopping_pos_amt", Float64), function="mean", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("grocery_pos_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("grocery_pos_amt", Float64), function="mean", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("shopping_net_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("shopping_net_amt", Float64), function="mean", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("food_dining_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("food_dining_amt", Float64), function="mean", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("entertainment_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("entertainment_amt", Float64), function="mean", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("personal_care_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("personal_care_amt", Float64), function="mean", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("health_fitness_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("health_fitness_amt", Float64), function="mean", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("misc_pos_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("misc_pos_amt", Float64), function="mean", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("misc_net_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("misc_net_amt", Float64), function="mean", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("travel_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("travel_amt", Float64), function="mean", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("grocery_net_amt", Float64), function="sum", time_window=timedelta(days=30)),
        Aggregate(input_column=Field("grocery_net_amt", Float64), function="mean", time_window=timedelta(days=30))
    ]
)
def user_transaction_aggregates_by_category(transactions_df):
    from pyspark.sql.functions import col, when

    df = transactions_df

    for cat in CATEGORIES:
        df = df.withColumn(f'{cat}_amt', when(col('category') == cat, col('amt')).otherwise(None))

    final_columns = ['user_id', 'timestamp'] + [f'{cat}_amt' for cat in CATEGORIES]
    df = df.select(*final_columns)
    return df
