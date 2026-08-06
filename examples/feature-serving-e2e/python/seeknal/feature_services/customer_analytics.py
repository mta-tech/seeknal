from seeknal.pipeline import FeatureView, feature_service


@feature_service(
    name="customer_analytics",
    version="1",
    variant="default",
    owner="ml-platform",
    description="Applied customer features for model inference",
    consumer="churn-model",
    tags=["customer", "production"],
    views=[
        FeatureView(
            ref="feature_group.customer_profile",
            features=["age", "lifetime_value"],
        )
    ],
)
def customer_analytics():
    pass
