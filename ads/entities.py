from tecton import Entity


ad = Entity(
    name='ad',
    default_join_keys=['ad_id'],
    description='An ad',
    family='ads',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)

content = Entity(
    name="content",
    join_keys=["content_id"],
    description='Content ID',
    family='ads',
    owner='rohit@tecton.ai',
    tags={'release': 'production'}
)

auction = Entity(
    name="auction",
    join_keys=["auction_id"],
    description='Auction ID',
    family='ads',
    owner='derek@tecton.ai',
)

user = Entity(
    name='ads_user',
    default_join_keys=['user_id'],
    description='A user of the platform',
    family='ads',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)
