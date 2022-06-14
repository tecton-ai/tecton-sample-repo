from fraud.features.on_demand_feature_views.user_age import user_age


# Testing the 'user_age' feature which takes in request data ('timestamp')
# and a precomputed feature ('USER_DATE_OF_BIRTH') as inputs
def test_user_age():
    user_date_of_birth = {'USER_DATE_OF_BIRTH': '1992-12-05'}
    request = {'timestamp': '2021-05-14T00:00:00.000+00:00'}

    actual = user_age.run(request=request, user_date_of_birth=user_date_of_birth)
    expected = {'user_age': 10387}
    assert actual == expected
