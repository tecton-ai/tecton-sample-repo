from fraud.features.realtime_features.complex_data_type import complex_data_type_odfv
import pytest


@pytest.mark.parametrize(
    "input_data,expected_output",
    [
        (
            {
                "string_map": {"key1": "value1", "key2": "value2"},
                "two_dimensional_array": [["a", "b"], ["c", "d"]],
                "simple_struct": {"string_field": "test", "float64_field": 1.23}
            },
            {
                "output_string_map": {"key1": "value1", "key2": "value2", "new_key": "new_value"},
                "output_two_dimensional_array": [["a", "b"], ["c", "d"], ["value"]],
                "output_simple_struct": {"string_field": None, "float64_field": 1.23}
            }
        ),
        (
            {
                "string_map": {},
                "two_dimensional_array": [],
                "simple_struct": {"string_field": "", "float64_field": 0.0}
            },
            {
                "output_string_map": {"new_key": "new_value"},
                "output_two_dimensional_array": [["value"]],
                "output_simple_struct": {"string_field": None, "float64_field": 0.0}
            }
        ),
    ],
)
def test_complex_data_type(input_data, expected_output):
    actual = complex_data_type_odfv.run_transformation(input_data={"request": input_data})
    assert actual == expected_output 