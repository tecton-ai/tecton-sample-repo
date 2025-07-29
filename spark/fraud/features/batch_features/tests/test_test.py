from datetime import datetime

def test_create_dataframe(tecton_pytest_spark_session):
    signup_timestamp = datetime(2022, 5, 1)

    data = [
        ("user_1", signup_timestamp, 1000000000000000),
        ("user_2", signup_timestamp, 4000000000000000),
        ("user_3", signup_timestamp, 5000000000000000),
        ("user_4", signup_timestamp, 6000000000000000)
    ]

    # Define schema: user_id, signup_timestamp, cc_num
    schema = ["user_id", "signup_timestamp", "cc_num"]

    # Convert to Spark DataFrame
    input_spark_df = tecton_pytest_spark_session.createDataFrame(data, schema)