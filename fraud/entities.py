from tecton import Entity
from tecton.types import String, Field, Int64


user = Entity(
    name='fraud_user',
    join_keys=[Field('user_id', String)],
    description='A user of the platform',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)

merchant = Entity(
    name='merchant',
    # Both pass `tecton plan`
    join_keys=[Field('merchant', String)],
    # join_keys=[Field('merchant', Int64)],
    description='A merchant',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)
