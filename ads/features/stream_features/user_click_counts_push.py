from datetime import datetime, timedelta
from tecton import PushSource, Entity, stream_feature_view, BatchTriggerType, FeatureService
from tecton.types import Timestamp, String, Array, Float64, Field

user = Entity(
    name='user',
    join_keys=['user_id'],
    description='A user of the platform',
    owner='achal@tecton.ai',
    tags={'release': 'production'}
)


input_schema = [
    Field(name='user_id', dtype=String),
    Field(name='timestamp', dtype=Timestamp),
    Field(name='transactions', dtype=Array(Float64)),
]
transactions_source = PushSource(
    name="transactions_source",
    schema=input_schema,
    description="""
        A push source for synchronous, online ingestion of historical transaction events.
    """,
    owner="achal@tecton.ai",
    tags={'release': 'staging'}
)

output_schema = [
    Field(name='user_id', dtype=String),
    Field(name='timestamp', dtype=Timestamp),
    Field(name='max_transaction_amt', dtype=Float64),
]


@stream_feature_view(
    name="transactions",
    source=transactions_source,
    entities=[user],
    online=True,
    offline=True,
    feature_start_time=datetime(2023, 1, 1),
    ttl=timedelta(days=30),
    tags={'release': 'production'},
    owner='achal@tecton.ai',
    description='The transactions for a user',
    mode='python',
    schema=output_schema,
    batch_trigger=BatchTriggerType.NO_BATCH_MATERIALIZATION,
)
def max_transactions(transactions_source):
    transactions = transactions_source["transactions"]
    max_transaction_amt = max(transactions)
    del transactions_source["transactions"]
    transactions_source["max_transaction_amt"] = max_transaction_amt

    return transactions_source


fs = FeatureService(
    name="transaction_fs",
    online_serving_enabled=True,
    features=[max_transactions],
    tags={'release': 'production'},
    owner='achal@tecton.ai',
)