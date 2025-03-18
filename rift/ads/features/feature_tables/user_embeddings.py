from tecton.types import Array, Float64
from tecton import FeatureTable, Attribute
from ads.entities import user
from datetime import timedelta


user_embeddings = FeatureTable(
    name='user_embeddings',
    entities=[user],
    features=[
        Attribute(name='user_embedding', dtype=Array(Float64)),
    ],
    timestamp_field="timestamp",
    online=True,
    offline=True,
    ttl=timedelta(days=10),
    description='Precomputed user embeddings pushed into Tecton.',
    owner='demo-user@tecton.ai'
)
