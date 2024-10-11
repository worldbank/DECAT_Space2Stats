import os

from aws_cdk import App, Environment
from aws_stack import Space2StatsStack
from settings import DeploymentSettings

settings = DeploymentSettings(
    _env_file=f"aws_deployment_{os.environ.get('STAGE', 'dev')}.env"
)

env = Environment(
    account=settings.CDK_DEFAULT_ACCOUNT, region=settings.CDK_DEFAULT_REGION
)

app = App()

Space2StatsStack(
    app, f"Space2Stats-{settings.STAGE}", env=env, deployment_settings=settings
)

app.synth()
