from tecton.types import Array, Float64
from tecton import FeatureTable, Attribute
from datetime import timedelta

from ads.entities import ad

ad_embeddings = FeatureTable(
    name='ad_embeddings',
    entities=[ad],
    features=[
        Attribute(name='ad_embedding', dtype=Array(Float64)),
    ],
    timestamp_field='timestamp',
    online=True,
    offline=True,
    ttl=timedelta(days=10),
    description='Precomputed ad embeddings pushed into Tecton.',
    owner='demo-user@tecton.ai'
)
