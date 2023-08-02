from tecton import Aggregation, on_demand_feature_view, RequestSource, spark_batch_config, batch_feature_view, BatchSource, Entity
from tecton.types import Field, Int64, Struct, Float64, String, Array
import json

# Input RequestSource Schema
input_schema = [Field("merch_lat", Float64), Field("merch_long", Float64),]
request_source = RequestSource(input_schema)

# Output Schema
output_schema = [Field('nested_array', Array(Array(Float64)))]

# ODFV
@on_demand_feature_view(
  sources=[request_source],
  mode='python',
  schema=output_schema
)
def my_odfv2(request):
  return {
          "nested_array": [[request["merch_lat"], request["merch_long"]], [request["merch_lat"], request["merch_long"]]],
  }
