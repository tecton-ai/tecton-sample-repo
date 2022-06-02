# from tecton import batch_feature_view, Input, tecton_sliding_window, transformation, const, BackfillConfig
# from ads.entities import user
# from ads.data_sources.ad_impressions import ad_impressions_batch
# from datetime import datetime
#
# # Counts distinct ad ids for each user and window. The timestamp
# # for the feature is the end of the window, which is set by using
# # the tecton_sliding_window transformation
# @transformation(mode='spark_sql')
# def user_distinct_ad_count_transformation(window_input_df):
#     return f'''
#         SELECT
#             user_uuid as user_id,
#             approx_count_distinct(ad_id) as distinct_ad_count,
#             window_end as timestamp
#         FROM
#             {window_input_df}
#         GROUP BY
#             user_uuid, window_end
#         '''
#
# @batch_feature_view(
#     inputs={'ad_impressions': Input(ad_impressions_batch, window='7d')},
#     entities=[user],
#     mode='pipeline',
#     ttl='1d',
#     batch_schedule='1d',
#     online=False,
#     offline=False,
#     feature_start_time=datetime(2021, 4, 1),
#     backfill_config=BackfillConfig("multiple_batch_schedule_intervals_per_job"),
#     family='ad_serving',
#     tags={'release': 'production'},
#     owner='david@tecton.ai',
#     description='How many distinct advertisements a user has been shown in the last week'
# )
# def user_distinct_ad_count_7d(ad_impressions):
#     return user_distinct_ad_count_transformation(
#         # Use tecton_sliding_transformation to create trailing 7 day time windows.
#         # The slide_interval defaults to the batch_schedule (1 day).
#         tecton_sliding_window(ad_impressions,
#             timestamp_key=const('timestamp'),
#             window_size=const('7d')))
