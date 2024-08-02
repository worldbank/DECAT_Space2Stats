from aws_cdk import App, Environment
from aws_stack import Space2StatsStack
from settings import settings

app = App()

env = Environment(
    account=settings.CDK_DEFAULT_ACCOUNT,
    region=settings.CDK_DEFAULT_REGION
)

Space2StatsStack(app, "Space2StatsStack", env=env)

app.synth()