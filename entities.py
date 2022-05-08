from tecton import Entity


user = Entity(
    name='user',
    join_keys=['user_id'],
    owner='matt@tecton.ai',
    tags={'release': 'production'},
    description='A user of the platform'
)
