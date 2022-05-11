from tecton import Entity, FeatureTable
from tecton.types import String, Timestamp, Int64
from fraud.entities import user


schema = [
    Field('user_id', String),
    Field('timestamp', Timestamp),
    Field('user_login_count_7d', Int64),
    Field('user_login_count_30d', Int64),
]

user_login_counts = FeatureTable(
    name='user_login_counts',
    entities=[user],
    schema=schema,
    online=False,
    offline=True,
    owner='derek@tecton.ai',
    tags={'release': 'production'},
    description='User login counts over time.'
)
