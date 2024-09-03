from tecton.types import Field, String, Timestamp, Array, Float64
from tecton import Entity, FeatureTable
from datetime import timedelta

from ads.entities import ad_embeddings_entity

schema = [
    Field('ad_id', String),
    Field('timestamp', Timestamp),
    Field('ad_embedding', Array(Float64))
]


ad_embeddings = FeatureTable(
    name='ad_embeddings',
    entities=[ad_embeddings_entity],
    schema=schema,
    online=True,
    offline=True,
    ttl=timedelta(days=10),
    description='Precomputed ad embeddings pushed into Tecton.',
    owner='demo-user@tecton.ai'
)
