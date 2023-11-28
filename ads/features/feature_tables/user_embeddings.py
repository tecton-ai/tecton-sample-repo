from tecton.types import Field, String, Timestamp, Array, Float64
from tecton import Entity, FeatureTable, DeltaConfig
from ads.entities import user
from datetime import timedelta


schema = [
    Field('user_id', String),
    Field('timestamp', Timestamp),
    Field('user_embedding', Array(Float64))
]


user_embeddings = FeatureTable(
    name='user_embeddings',
    entities=[user],
    schema=schema,
    online=True,
    offline=True,
    ttl=timedelta(days=10),
    description='Precomputed user embeddings pushed into Tecton.',
    owner='demo-user@tecton.ai'
)
