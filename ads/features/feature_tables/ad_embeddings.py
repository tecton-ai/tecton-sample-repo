from tecton import Attribute, Entity
from tecton import FeatureTable
from tecton.types import Field, String, Timestamp, Array, Float64
from ads.entities import ad
from datetime import timedelta

schema = [
    Field('ad_id', String),
    Field('timestamp', Timestamp),
    Field('ad_embedding', Array(Float64))
]
#
ad2 = Entity(
    name='ad2',
    join_keys=[Field("ad_id", String)],
    description='An ad',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)

ad_embeddings = FeatureTable(
    name='ad_embeddings',
    entities=[ad2],
    features=[Attribute("ad_embedding", dtype=Array(Float64))],
    online=True,
    offline=True,
    ttl=timedelta(days=10),
    description='Precomputed ad embeddings pushed into Tecton.',
    owner='demo-user@tecton.ai',
    timestamp_field="timestamp"
)
