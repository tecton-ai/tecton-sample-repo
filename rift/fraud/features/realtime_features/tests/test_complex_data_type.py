import pytest
from fraud.features.realtime_features.complex_data_type import complex_data_type_odfv


def test_complex_data_type():
    # Test normal case with all complex types
    request = {
        'string_map': {'key1': 'value1', 'key2': 'value2'},
        'two_dimensional_array': [['a', 'b'], ['c', 'd']],
        'simple_struct': {
            'string_field': 'test',
            'float64_field': 1.23
        }
    }
    result = complex_data_type_odfv.run_transformation({'request': request})
    
    # Verify string map transformation
    assert 'output_string_map' in result
    assert result['output_string_map']['new_key'] == 'new_value'
    assert result['output_string_map']['key1'] == 'value1'
    assert result['output_string_map']['key2'] == 'value2'
    
    # Verify 2D array transformation
    assert 'output_two_dimensional_array' in result
    assert len(result['output_two_dimensional_array']) == 3
    assert result['output_two_dimensional_array'][-1] == ['value']
    
    # Verify struct transformation
    assert 'output_simple_struct' in result
    assert result['output_simple_struct']['string_field'] is None
    assert result['output_simple_struct']['float64_field'] == 1.23

    # Test edge case - empty inputs
    empty_request = {
        'string_map': {},
        'two_dimensional_array': [],
        'simple_struct': {
            'string_field': '',
            'float64_field': 0.0
        }
    }
    result = complex_data_type_odfv.run_transformation({'request': empty_request})
    
    # Verify empty string map transformation
    assert result['output_string_map']['new_key'] == 'new_value'
    assert len(result['output_string_map']) == 1
    
    # Verify empty 2D array transformation
    assert len(result['output_two_dimensional_array']) == 1
    assert result['output_two_dimensional_array'][0] == ['value']
    
    # Verify empty struct transformation
    assert result['output_simple_struct']['string_field'] is None
    assert result['output_simple_struct']['float64_field'] == 0.0 