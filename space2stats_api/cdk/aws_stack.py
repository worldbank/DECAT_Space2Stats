from aws_cdk import (
    Stack,
    aws_apigatewayv2 as apigatewayv2,
    aws_apigatewayv2_integrations as integrations,
    aws_lambda as _lambda,
    aws_certificatemanager as acm,
    Duration
)
from aws_cdk.aws_lambda_python_alpha import PythonFunction
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

        certificate = acm.Certificate.from_certificate_arn(self, "Certificate", deployment_settings.CDK_CERTIFICATE_ARN)

        domain_name = apigatewayv2.DomainName(
            self, "DomainName",
            domain_name=deployment_settings.CDK_DOMAIN_NAME,
            certificate=certificate
        )

        http_api = apigatewayv2.HttpApi(
            self, "Space2StatsHttpApi",
            default_integration=integrations.HttpLambdaIntegration(
                "LambdaIntegration",
                handler=lambda_function
            )
        )

        apigatewayv2.ApiMapping(
            self, "ApiMapping",
            api=http_api,
            domain_name=domain_name,
            stage=http_api.default_stage
        )