from aws_cdk import (
    Stack,
    aws_apigateway as apigateway,
    aws_lambda as _lambda
)
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from constructs import Construct
from settings import Settings

class Space2StatsStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        settings = Settings(_env_file="./aws.env")

        lambda_function = PythonFunction(
            self, "Space2StatsFunction",
            entry="../src", 
            runtime=_lambda.Runtime.PYTHON_3_11,
            index="app/main.py", 
            handler="handler",
            environment=settings.model_dum
        )

        apigateway.LambdaRestApi(
            self, "Space2StatsAPI",
            handler=lambda_function,
            proxy=True
        )