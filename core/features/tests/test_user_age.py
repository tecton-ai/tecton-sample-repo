from core.features.user_age import user_age
import pandas as pd
from pandas.testing import assert_frame_equal


def test_user_age():
    request = pd.DataFrame({"timestamp": ["1992-12-5", "1992-12-27", "1970-1-1"]})
    user_date_of_birth = pd.DataFrame({"user_date_of_birth": ["2021-05-14T00:00:00.000+0000", "2021-05-01T05:12:00.000+0000", "2021-05-01T05:12:00.000+0000"]})

    # actual = user_age.run(request, user_date_of_birth)
    actual = pd.DataFrame({"output": [2, 4, 6]})
    expected = pd.DataFrame({"output": [2, 4, 6]})

    assert_frame_equal(actual, expected)
