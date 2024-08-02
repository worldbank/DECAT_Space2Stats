from aws_cdk import (
    Stack,
    aws_apigateway as apigateway,
    aws_lambda as _lambda
)
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from constructs import Construct
from settings import settings

class Space2StatsStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        lambda_function = PythonFunction(
            self, "Space2StatsFunction",
            entry="../app", 
            runtime=_lambda.Runtime.PYTHON_3_11,
            index="main.py", 
            handler="handler", 
            environment={
                "DB_HOST": settings.DB_HOST,
                "DB_PORT": str(settings.DB_PORT),
                "DB_NAME": settings.DB_NAME,
                "DB_USER": settings.DB_USER,
                "DB_PASSWORD": settings.DB_PASSWORD,
            },
        )

        apigateway.LambdaRestApi(
            self, "Space2StatsAPI",
            handler=lambda_function,
            proxy=True
        )