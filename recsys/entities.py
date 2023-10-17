from tecton import Entity

article = Entity(
    name='article',
    join_keys=['aid'],
    description='Item on an ecommerce site',
    owner='mihir@tecton.ai',
    tags={'release': 'production'}
)

session = Entity(
    name="session",
    join_keys=["session"],
    description='A user session',
    owner='mihir@tecton.ai',
    tags={'release': 'production'}
)