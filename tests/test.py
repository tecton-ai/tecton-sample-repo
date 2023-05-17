from datetime import datetime
from datetime import timezone
import pytest
import pandas

from pyspark.sql.functions import col

from tests.test_utils import write_table, assert_frame_equivalent
from fcos import mock_batch_source


@pytest.fixture(scope="class", autouse=True)
def create_spark_data(my_custom_spark_session):
    cols = ["user_uuid", "created_at", "transaction_amount"]
    data = [
        ["1", "2023-04-14T23:59:59Z", 100],
        ["1", "2023-04-15T00:00:00Z", 200],
        ["1", "2023-04-15T00:00:01Z", 300],
        ["2", "2023-04-15T23:59:58Z", 400],
        ["2", "2023-04-15T23:59:59Z", 500],
        ["2", "2023-04-16T00:00:00Z", 600],
    ]
    df = my_custom_spark_session.createDataFrame(data, cols)
    df = df.withColumn("created_at", col("created_at").cast("timestamp"))
    write_table(my_custom_spark_session, df, database='my_database', table='transactions')

def test_spark_batch_config():
    mock_batch_source.validate() # Will call the Tecton backend.
    actual = mock_batch_source.get_dataframe(start_time=datetime(2023, 4, 15, tzinfo=timezone.utc) , end_time=datetime(2023, 4, 16, tzinfo=timezone.utc)).to_pandas()

    print(actual)

    expected = pandas.DataFrame({
        "user_uuid": ["1", "1", "2", "2"],
        "created_at":  [datetime(2023, 4, 15), datetime(2023, 4, 15, 0, 0, 1), datetime(2023, 4, 15, 23, 59, 58), datetime(2023, 4, 15, 23, 59, 59)],
        "transaction_amount": [200, 300, 400, 500],
    })

    assert_frame_equivalent(actual, expected, sort_cols=["user_uuid", "created_at"])


