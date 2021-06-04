from fraud.features.user_age import user_age
import pandas as pd
from pandas.testing import assert_frame_equal

# Testing the 'user_age' feature which takes in request data ('timestamp')
# and a precomputed feature ('user_date_of_birth') as inputs
def test_user_age():
    user_date_of_birth = pd.DataFrame({'user_date_of_birth': ['1992-12-5', '1992-12-27', '1970-1-1']})
    request = pd.DataFrame({'timestamp': ['2021-05-14T00:00:00.000+0000', '2021-05-01T00:00:00.000+0000', '2021-05-01T00:00:00.000+0000']})

    actual = user_age.run(request=request, user_date_of_birth=user_date_of_birth)
    expected = pd.DataFrame({'user_age': [10387, 10352, 18748]})

    assert_frame_equal(actual, expected)
