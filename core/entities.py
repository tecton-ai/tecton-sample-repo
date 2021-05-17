from tecton import Entity


user = Entity(
    name="User",
    default_join_keys=["user_id"],
    description="A user of the platform",
    family="core",
    owner="matt@tecton.ai",
    tags={"release": "production"}
)
