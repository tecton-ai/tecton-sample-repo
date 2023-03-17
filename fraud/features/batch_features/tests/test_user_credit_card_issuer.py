from datetime import datetime, timedelta
import pytest
import pandas
from fraud.features.batch_features.user_credit_card_issuer import user_credit_card_issuer


# The `tecton_pytest_spark_session` is a PyTest fixture that provides a
# Tecton-defined PySpark session for testing Spark transformations and feature
# views.
@pytest.mark.skip(reason="Requires JDK installation and $JAVA_HOME env variable to run.")
def test_user_credit_card_issuer(tecton_pytest_spark_session):
    input_pandas_df = pandas.DataFrame({
        "user_id": ["user_1", "user_2", "user_3", "user_4"],
        "signup_timestamp": [datetime(2022, 5, 1)] * 4,
        "cc_num": [1000000000000000, 4000000000000000, 5000000000000000, 6000000000000000],
    })
    input_spark_df = tecton_pytest_spark_session.createDataFrame(input_pandas_df)

    # Simulate materializing features for May 1st.
    output = user_credit_card_issuer.test_run(
        spark=tecton_pytest_spark_session,
        start_time=datetime(2022, 5, 1),
        end_time=datetime(2022, 5, 2),
        fraud_users_batch=input_spark_df)

    actual = output.toPandas()

    expected = pandas.DataFrame({
        "user_id": ["user_1", "user_2", "user_3", "user_4"],
        "signup_timestamp":  [datetime(2022, 5, 1)] * 4,
        "credit_card_issuer": ["other", "Visa", "MasterCard", "Discover"],
    })

    pandas.testing.assert_frame_equal(actual, expected)
