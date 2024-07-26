from tecton import Entity
from tecton.types import Field, String

ad = Entity(
    name='ad',
    join_keys=[Field('ad_id', String)],
    description='An ad',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)

content = Entity(
    name="content",
    join_keys=[Field('content_id', String)],
    description='Content ID',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)

auction = Entity(
    name="auction",
    join_keys=[Field('auction_id', String)],
    description='Auction ID',
    owner='demo-user@tecton.ai',
)

user = Entity(
    name='ads_user',
    join_keys=[Field('content_keyword', String)],
    description='A user of the platform',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)

content_keyword = Entity(
    name='ContentKeyword',
    join_keys=[Field('content_keyword', String)],
    description='The keyword describing the content this ad is being placed alongside.',
    owner='demo-user@tecton.ai',
    tags={'release': 'production'}
)
