from tecton import batch_window_aggregate_feature_view, Input, transformation, FeatureAggregation, NewDatabricksClusterConfig, const
from core.entities import user
from core.data_sources.users_batch import users_batch
from fraud.data_sources.transactions_batch import transactions_batch
from datetime import datetime

# Transformation that calculates the age of the specified account at transaction time.
# Requires transactions table for transaction time, and user table for account creation time.
@transformation(mode="spark_sql")
def account_age_at_transaction_time(transactions, users, entity_id_column, join_id_column):
    return f'''
        SELECT
            {transactions}.{entity_id_column} AS user_id,
            {transactions}.timestamp,
            datediff({transactions}.timestamp, {users}.signup_date) AS account_age_days
        FROM {transactions}
        JOIN {users} ON {transactions}.{join_id_column} = {users}.user_id
    '''

@batch_window_aggregate_feature_view(
    # Include both inputs that will be joined in the transformation
    inputs={'transactions': Input(transactions_batch),
            'users': Input(users_batch)},
    entities=[user],
    mode='pipeline',
    aggregation_slide_period='4h',
    aggregations=[FeatureAggregation(column='account_age_days', function='mean', time_windows=['4h','24h','72h','168h']),
                  FeatureAggregation(column='account_age_days', function='max', time_windows=['4h','24h','72h','168h'])],
    online=True,
    offline=True,
    feature_start_time=datetime(2020, 10, 10),
    family='fraud',
    tags={'release': 'production'},
    owner='derek@tecton.ai',
    description='Features based on the age of accounts this user has transferred to. Users frequently transferring to newly created accounts may have been compromised.',
    batch_materialization=NewDatabricksClusterConfig(instance_type="r5.large", number_of_workers=3)
)
def user_transaction_destination_account_age(transactions, users):
    # Specify that our entity is the origination account, and calculating age of destinatio accounts
    return account_age_at_transaction_time(transactions, users, const("nameorig"), const("namedest"))

@batch_window_aggregate_feature_view(
    # Include both inputs that will be joined in the transformation
    inputs={'transactions': Input(transactions_batch),
            'users': Input(users_batch)},
    entities=[user],
    mode='pipeline',
    aggregation_slide_period='4h',
    aggregations=[FeatureAggregation(column='account_age_days', function='mean', time_windows=['4h','24h','72h','168h']),
                  FeatureAggregation(column='account_age_days', function='max', time_windows=['4h','24h','72h','168h'])],
    online=True,
    offline=True,
    feature_start_time=datetime(2020, 10, 10),
    family='fraud',
    tags={'release': 'production'},
    owner='derek@tecton.ai',
    description='Features based on the age of accounts this user has received from. Users frequently receiving from new accounts may be fraudulent.',
    batch_materialization=NewDatabricksClusterConfig(instance_type="r5.large", number_of_workers=3)
)
def user_transaction_origin_account_age(transactions, users):
    # Specify that our entity is the destination account, and calculating age of origin accounts
    return account_age_at_transaction_time(transactions, users, const("namedest"), const("nameorig"))
