from tecton import spark_batch_config, BatchSource


@spark_batch_config()
def dummy_batch_config(spark):
    import pandas
    import datetime

    return spark.createDataFrame(pandas.DataFrame.from_records(
        [
            {
                "user_id": "user_1",
                "timestamp": datetime.datetime(2022, 12, 1, 1),
                "int_feature": 1
            },
            {
                "user_id": "user_2",
                "timestamp": datetime.datetime(2022, 12, 1, 2),
                "int_feature": 2
            },
            {
                "user_id": "user_3",
                "timestamp": datetime.datetime(2022, 12, 1, 3),
                "int_feature": 3
            },
        ]
    ))


dummy_batch_source = BatchSource(name="dummy_batch_source", batch_config=dummy_batch_config)
