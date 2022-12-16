from tecton import RequestSource, on_demand_feature_view
from tecton.types import String, Timestamp, Int64, Float64, Field, Bool, Array
import batch_feature_views
import feature_tables

output_schema = [Field('output_int_feature', Int64)]

@on_demand_feature_view(
    sources=[batch_feature_views.bfv_materialization_enabled],
    mode='python',
    schema=output_schema,
)
def odfv_with_materialized_bfv(bfv):
    return {
        'output_int_feature': bfv['int_feature']
    }

@on_demand_feature_view(
    sources=[batch_feature_views.bfv_materialization_disabled],
    mode='python',
    schema=output_schema,
)
def odfv_with_unmaterialized_bfv(bfv):
    return {
        'output_int_feature': bfv['int_feature']
    }

@on_demand_feature_view(
    sources=[feature_tables.ft_materialization_enabled],
    mode='python',
    schema=output_schema,
)
def odfv_with_materialized_ft(ft):
    return {
        'output_int_feature': ft['int_feature']
    }

@on_demand_feature_view(
    sources=[feature_tables.ft_materialization_disabled],
    mode='python',
    schema=output_schema,
)
def odfv_with_unmaterialized_ft(ft):
    return {
        'output_int_feature': ft['int_feature']
    }

@on_demand_feature_view(
    sources=[RequestSource(schema=[Field('input_int_feature_1', Int64)])],
    mode='python',
    schema=output_schema,
)
def odfv_with_request_source_1(request):
    return {
        'output_int_feature': request['input_int_feature_1']
    }

@on_demand_feature_view(
    sources=[RequestSource(schema=[Field('input_int_feature_2', Int64)])],
    mode='python',
    schema=output_schema,
)
def odfv_with_request_source_2(request):
    return {
        'output_int_feature': request['input_int_feature_2']
    }

@on_demand_feature_view(
    sources=[RequestSource(schema=[Field('input_int_feature_2', Int64)])],
    mode='python',
    schema=[Field('output_array_feature', Array(Int64))],
)
def odfv_with_request_source_3(request):
    return {
        'output_array_feature': [request['input_int_feature_2'], 0, 0, 1]
    }
