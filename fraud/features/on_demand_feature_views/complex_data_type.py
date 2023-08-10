from tecton import on_demand_feature_view
from tecton import RequestSource
from tecton.types import Array
from tecton.types import Field
from tecton.types import String
from tecton.types import Struct
from tecton.types import Map
from tecton.types import Float64

request_source = RequestSource(
    [
        Field("string_map", Map(String, String)),
        Field("two_dimensional_array", Array(Array(String))),
        Field("simple_struct", Struct(
          [
              Field("string_field", String),
              Field("float64_field", Float64),
          ]
        )),
    ]
)

output_schema = [
        Field("output_string_map", Map(String, String)),
        Field("output_two_dimensional_array", Array(Array(String))),
        Field("output_simple_struct", Struct(
          [
              Field("string_field", String),
              Field("float64_field", Float64),
          ]
        )),
]

@on_demand_feature_view(mode="python", sources=[request_source], schema=output_schema)
def complex_data_type_odfv(request):
    return {
        "output_string_map": request["string_map"],
        "output_two_dimensional_array": request["two_dimensional_array"],
        "output_simple_struct": request["simple_struct"],
    }
