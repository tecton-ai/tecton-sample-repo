from tecton import Entity

user = Entity(
    name='fraud_user',
    join_keys=['user_id'],
    description='A user of the platform',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)