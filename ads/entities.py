from tecton import Entity


ad = Entity(
    name='Ad',
    default_join_keys=['ad_id'],
    description='An ad',
    family='ads',
    owner='matt@tecton.ai',
    tags={'release': 'production'}
)


# partner_entity = Entity(name='PartnerWebsite', default_join_keys=['partner_id'], description='The partner website participating in the ad network.')
# content_keyword_entity = Entity(name='ContentKeyword', default_join_keys=['content_keyword'], description='The keyword describing the content this ad is being placed alongside.')
# ad_campaign_entity = Entity(name='AdCampaign', default_join_keys=['ad_campaign_id'], description='The ad campaign')
# ad_group_entity = Entity(name='AdGroup', default_join_keys=['ad_group_id'], description='The ad group')
# ad_content_entity = Entity(name='AdContent', default_join_keys=['ad_content_id'], description='The content of an ad')
