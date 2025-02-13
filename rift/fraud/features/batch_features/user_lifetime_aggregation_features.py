from tecton import batch_feature_view, Aggregate, LifetimeWindow
from tecton.types import Field, Float32
from fraud.data_sources.transactions import transactions_batch
from fraud.entities import user
from datetime import datetime, timedelta

# Support for Rift Lifetime features coming soon
# @batch_feature_view(
#     sources=[transactions_batch],
#     description="Lifetime transaction aggregation features",
#     mode="pandas",
#     entities=[user],
#     compaction_enabled=True,
#     batch_schedule=timedelta(days=1),
#     lifetime_start_time=datetime(2000, 1, 1), # Start of the lifetime
#     timestamp_field="timestamp",
#     features=[
#         Aggregate(
#             input_column=Field("amount", Float32), 
#             function="max", 
#             time_window=LifetimeWindow(),
#             description="Lifetime max transaction amount of a user"),
#         Aggregate(
#             input_column=Field("amount", Float32), 
#             function="sum", 
#             time_window=LifetimeWindow(),
#             description="Lifetime sum transaction amount of a user")
#     ],
# )
# def user_average_transaction_amount(transactions):
#     return transactions[['user_id', 'timestamp', 'amount']]
