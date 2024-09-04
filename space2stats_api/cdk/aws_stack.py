from aws_cdk import Duration, Stack
from aws_cdk import aws_apigatewayv2 as apigatewayv2
from aws_cdk import aws_apigatewayv2_integrations as integrations
from aws_cdk import aws_certificatemanager as acm
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_lambda as _lambda
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from constructs import Construct
from settings import AppSettings, DeploymentSettings


class Space2StatsStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        app_settings = AppSettings(_env_file="./aws_app.env")
        deployment_settings = DeploymentSettings(_env_file="./aws_deployment.env")

        bucket = s3.Bucket(
            self, "LargeResponseBucket",
            lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(1))],
        )

        lambda_function = PythonFunction(
            self,
            "Space2StatsFunction",
            entry="../src",
            runtime=_lambda.Runtime.PYTHON_3_11,
            index="space2stats/handler.py",
            timeout=Duration.seconds(120),
            handler="handler",
            environment={
                "S3_BUCKET_NAME": bucket.bucket_name,
                **app_settings.model_dump(),
            },
            memory_size=1024,
        )

        bucket.grant_read_write(lambda_function)

        certificate = acm.Certificate.from_certificate_arn(
            self, "Certificate", deployment_settings.CDK_CERTIFICATE_ARN
        )

        domain_name = apigatewayv2.DomainName(
            self,
            "DomainName",
            domain_name=deployment_settings.CDK_DOMAIN_NAME,
            certificate=certificate,
        )

        http_api = apigatewayv2.HttpApi(
            self,
            "Space2StatsHttpApi",
            default_integration=integrations.HttpLambdaIntegration(
                "LambdaIntegration", handler=lambda_function
            ),
        )

        apigatewayv2.ApiMapping(
            self,
            "ApiMapping",
            api=http_api,
            domain_name=domain_name,
            stage=http_api.default_stage,
        )
