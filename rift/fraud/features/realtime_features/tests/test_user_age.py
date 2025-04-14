from fraud.features.realtime_features.user_age import user_age


# Testing the 'user_age' feature which takes in request data ('timestamp')
# and a precomputed feature ('user_date_of_birth') as inputs
def test_user_age():
    # Prepare input data
    input_data = {
        'request': {'timestamp': '2021-05-14T00:00:00.000+00:00'},
        'user_date_of_birth': {'user_date_of_birth': '1992-12-05'}
    }

    actual = user_age.run_transformation(input_data=input_data)
    expected = {'user_age': 10387}
    assert actual == expected
