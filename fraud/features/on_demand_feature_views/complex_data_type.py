from tecton import on_demand_feature_view, Attribute
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

features = [
        Attribute("output_string_map", Map(String, String)),
        Attribute("output_two_dimensional_array", Array(Array(String))),
        Attribute("output_simple_struct", Struct([
              Field("string_field", String),
              Field("float64_field", Float64),
          ]
        )),
]

@on_demand_feature_view(mode="python", sources=[request_source], features=features)
def complex_data_type_odfv(request):
    # Transform map value 
    output_string_map = request["string_map"]
    output_string_map["new_key"] = "new_value"

    # Transform array value
    output_two_dimensional_array = request["two_dimensional_array"]
    output_two_dimensional_array.append(["value"])

    # Transform struct value
    output_simple_struct = request["simple_struct"]
    output_simple_struct["string_field"] = None
    
    return {
        "output_string_map": output_string_map,
        "output_two_dimensional_array": output_two_dimensional_array,
        "output_simple_struct": output_simple_struct,
    }
