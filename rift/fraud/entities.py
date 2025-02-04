from tecton import Entity
from tecton.types import Field, String

user = Entity(
    name='fraud_user',
    join_keys=[Field('user_id', String)],
    description='A user of the platform',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)

merchant = Entity(
    name='merchant',
    join_keys=[Field('merchant', String)],
    description='A merchant',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)
