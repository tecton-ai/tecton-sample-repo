from datetime import datetime, timedelta
import pytest
import pandas
from fraud.features.batch_features.user_distinct_merchant_transaction_count_30d import user_distinct_merchant_transaction_count_30d


# The `tecton_pytest_spark_session` is a PyTest fixture that provides a
# Tecton-defined PySpark session for testing Spark transformations and feature
# views.
@pytest.mark.skip(reason="Requires JDK installation and $JAVA_HOME env variable to run.")
def test_user_distinct_merchant_transaction_count_30d(tecton_pytest_spark_session):
    input_pandas_df = pandas.DataFrame({
        "user_id": ["user_1", "user_1", "user_2"],
        "merchant": ["merchant_1", "merchant_2", "merchant_1"],
        "timestamp": [datetime(2022, 4, 10), datetime(2022, 4, 25), datetime(2022, 5, 1)],
    })
    input_spark_df = tecton_pytest_spark_session.createDataFrame(input_pandas_df)

    # Simulate materializing features for May 1st.
    output = user_distinct_merchant_transaction_count_30d.local_run(
        spark=tecton_pytest_spark_session,
        start_time=datetime(2022, 5, 1),
        end_time=datetime(2022, 5, 2),
        transactions_batch=input_spark_df)

    actual = output.toPandas()

    expected = pandas.DataFrame({
        "user_id": ["user_1", "user_2"],
        "timestamp": [datetime(2022, 5, 2) - timedelta(microseconds=1), datetime(2022, 5, 2) - timedelta(microseconds=1)],
        "distinct_merchant_transaction_count_30d": [2, 1],
    })

    pandas.testing.assert_frame_equal(actual, expected)