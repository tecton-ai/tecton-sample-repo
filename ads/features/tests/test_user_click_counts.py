# define test feature view
def test_user_ad_group_impression_count_7_days_transformer(spark_session):
    from ads.features.user_click_counts import user_click_counts
    assert user_click_counts is not None

    l = [

        ("id1", "ad_id_1", "2020-10-28 05:02:11", True),
        ("id2", "ad_id_1", "2020-10-28 05:02:11", False),

    ]
    input_df = spark_session.createDataFrame(l, ["user_uuid", "ad_group_id", "timestamp", "clicked"])

    output = user_click_counts.run(spark_session, ad_impressions=input_df).collect()
    print(f"Output: {output}")
    assert len(output) == 2
