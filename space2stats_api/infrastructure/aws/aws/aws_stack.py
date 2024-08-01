from aws_cdk import (
    aws_apigateway as apigateway,
    aws_lambda as _lambda,
    CfnOutput,
    Stack
)
from constructs import Construct

import os
from dotenv import load_dotenv

class AwsStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Load environment variables from .env file
        load_dotenv(dotenv_path="../../db.env")

        # Define the AWS Lambda function
        function = _lambda.Function(
            self, "Space2StatsFunction",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="main.handler",
            code=_lambda.Code.from_asset("../../app"),
            environment={
                "DB_HOST": os.getenv("DB_HOST"),
                "DB_PORT": os.getenv("DB_PORT"),
                "DB_NAME": os.getenv("DB_NAME"),
                "DB_USER": os.getenv("DB_USER"),
                "DB_PASSWORD": os.getenv("DB_PASSWORD"),
                "DB_TABLE_NAME": os.getenv("DB_TABLE_NAME")
            }
        )

        # Define API Gateway to expose the Lambda function
        api = apigateway.LambdaRestApi(
            self, "Space2StatsAPI",
            handler=function,
            proxy=True
        )

        CfnOutput(
            self, "APIEndpoint",
            value=api.url
        )