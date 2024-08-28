from tecton import Entity
from tecton.types import Field, String

article = Entity(
    name='article',
    join_keys=[Field('aid', String)],
    description='Item on an ecommerce site',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)

session = Entity(
    name="session",
    join_keys=[Field("session", String)],
    description='A user session',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)