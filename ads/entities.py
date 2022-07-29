from tecton import Entity


ad = Entity(
    name='ad',
    join_keys=['ad_id'],
    description='An ad',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)

content = Entity(
    name="content",
    join_keys=["content_id"],
    description='Content ID',
    owner='rohit@tecton.ai',
    tags={'release': 'production'}
)

auction = Entity(
    name="auction",
    join_keys=["auction_id"],
    description='Auction ID',
    owner='derek@tecton.ai',
)

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
