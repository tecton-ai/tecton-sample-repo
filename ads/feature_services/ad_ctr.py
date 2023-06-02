from tecton import FeatureService, RemoteComputeConfig
from ads.features.stream_features.content_keyword_clicks_push import content_keyword_click_counts_pandas
from ads.features.stream_features.content_keyword_clicks_push import content_keyword_click_counts_python
from ads.features.stream_features.content_keyword_clicks_push import content_keyword_click_counts_wafv




ad_ctr_feature_service = FeatureService(
    name='push_fs',
    description='A FeatureService providing features for a model that predicts if a user will click an ad.',
    tags={'release': 'production'},
    owner='t-rex@tecton.ai',
    online_serving_enabled=True,
    online_compute=RemoteComputeConfig(),
    features=[
        content_keyword_click_counts_pandas,
        content_keyword_click_counts_python,
        content_keyword_click_counts_wafv
    ],

)
