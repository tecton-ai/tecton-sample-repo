from tecton import Entity, FeatureTable, Attribute
from tecton.types import String, Timestamp, Int64, Field
from fraud.entities import user
from datetime import timedelta


schema = [
    Field('user_id', String),
    Field('timestamp', Timestamp),
    Field('user_login_count_7d', Int64),
    Field('user_login_count_30d', Int64),
]

user_login_counts = FeatureTable(
    name='user_login_counts',
    entities=[user],
    timestamp_field="timestamp",
    features=[
        Attribute("user_login_count_7d", dtype=Int64),
        Attribute("user_login_count_30d", dtype=Int64)
    ],
    online=True,
    offline=True,
    ttl=timedelta(days=7),
    owner='demo-user@tecton.ai',
    tags={'release': 'production'},
    description='User login counts over time.',
)
