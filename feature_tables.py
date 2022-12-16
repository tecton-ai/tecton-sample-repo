from tecton import Entity, FeatureTable
from tecton.types import String, Timestamp, Int64, Field
import entities
import datetime


schema = [
    Field('user_id', String),
    Field('timestamp', Timestamp),
    Field('int_feature', Int64),
]

ft_materialization_enabled = FeatureTable(
    name='ft_materialization_enabled',
    entities=[entities.user],
    schema=schema,
    online=True,
    offline=True,
    ttl=datetime.timedelta(days=7),
)

ft_materialization_disabled = FeatureTable(
    name='ft_materialization_disabled',
    entities=[entities.user],
    schema=schema,
    online=True,
    offline=False,
    ttl=datetime.timedelta(days=7),
)
