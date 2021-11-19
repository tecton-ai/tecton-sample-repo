import datetime
import pyspark
from fraud.features.batch_feature_views.user_has_good_credit_pyspark import user_has_good_credit_pyspark


def test_monthly_impression_count(tecton_pytest_spark_session):
    mock_data = [
        ('user_id1', "2020-10-28 05:02:11", 700),
        ('user_id2', "2020-10-28 05:02:11", 650)
    ]
    input_df = tecton_pytest_spark_session.createDataFrame(mock_data, ['user_id', 'timestamp', 'credit_score'])

    output = user_has_good_credit_pyspark.run(tecton_pytest_spark_session, credit_scores=input_df)
    output = output.toPandas()

    vals = output.values.tolist()

    expected = [['user_id1', 1, '2020-10-28 05:02:11'], ['user_id2', 0, '2020-10-28 05:02:11']]

    assert vals == expected
