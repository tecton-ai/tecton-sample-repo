from tecton import Entity

user = Entity(
    name='ads_user',
    join_keys=['user_id'],
    description='A user of the platform',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)

content_keyword = Entity(
    name='ContentKeyword',
    join_keys=['content_keyword'],
    description='The keyword describing the content this ad is being placed alongside.',
    owner='ravi@tecton.ai',
    tags={'release': 'production'}
)
