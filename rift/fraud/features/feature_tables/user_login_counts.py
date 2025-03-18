from tecton import FeatureTable, Attribute
from tecton.types import Int64
from fraud.entities import user
from datetime import timedelta


features = [
    Attribute('user_login_count_7d', Int64),
    Attribute('user_login_count_30d', Int64),
]

user_login_counts = FeatureTable(
    name='user_login_counts',
    entities=[user],
    features=features,
    online=True,
    offline=True,
    ttl=timedelta(days=7),
    owner='demo-user@tecton.ai',
    tags={'release': 'production'},
    description='User login counts over time.',
    timestamp_field='timestamp'
)
