from aws_cdk import (
    Stack,
    aws_apigateway as apigateway,
    aws_lambda as _lambda,
    aws_certificatemanager as acm
)
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from aws_cdk import Duration
from constructs import Construct
from settings import AppSettings, DeploymentSettings

class Space2StatsStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        app_settings = AppSettings(_env_file="./aws_app.env")
        deployment_settings = DeploymentSettings(_env_file="./aws_deployment.env")

        lambda_function = PythonFunction(
            self, "Space2StatsFunction",
            entry="../src", 
            runtime=_lambda.Runtime.PYTHON_3_11,
            index="app/main.py", 
            timeout=Duration.seconds(120),
            handler="handler",
            environment=app_settings.model_dump()
        )

        certificate = acm.Certificate.from_certificate_arn(self, "certificate", deployment_settings.CDK_CERTIFICATE_ARN)

        apigateway.LambdaRestApi(
            self, "Space2Stats",
            handler=lambda_function,
            proxy=True,
            domain_name=apigateway.DomainNameOptions(domain_name=deployment_settings.CDK_DOMAIN_NAME, certificate=certificate)
        )