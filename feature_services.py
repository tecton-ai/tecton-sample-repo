from tecton import FeatureService
import on_demand_feature_views

fs = FeatureService(
    name="my_fs",
    features=[
        on_demand_feature_views.odfv_with_request_source_1,
        on_demand_feature_views.odfv_with_request_source_2,
        on_demand_feature_views.odfv_with_request_source_3
    ]
)
