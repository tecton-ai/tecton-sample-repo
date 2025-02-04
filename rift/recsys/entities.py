from tecton import Entity
from tecton.types import Field, Int32

article = Entity(
    name='article',
    join_keys=[Field('aid', Int32)],
    description='Item on an ecommerce site',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)

session = Entity(
    name="session",
    join_keys=[Field("session", Int32)],
    description='A user session',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)