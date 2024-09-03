## API Deployment

This document provides a step-by-step guide to deploy the `space2stats` API using AWS CDK.

### 1. Dependencies Setup

Before proceeding with the deployment, ensure you have all necessary dependencies installed. Specifically, you’ll need:

- AWS CDK: Follow the installation guide [here](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html).
- AWS CLI: Ensure that the AWS CLI is installed and configured.

### 2. Setting Up the `space2stats` AWS Profile

To deploy the API, you need to configure the `space2stats` AWS profile. This profile allows the CDK to interact with the correct AWS account and region.

#### Step 1: Configure the AWS Profile

Edit the `~/.aws/config` file to add the following section:

```ini
[profile space2stats]
region = your-preferred-region
output = json
```

Replace `your-preferred-region` with your desired AWS region (e.g., `us-west-2`).

#### Step 2: Add AWS Credentials

Edit the `~/.aws/credentials` file to include the credentials for the `space2stats` profile:

```ini
[space2stats]
aws_access_key_id = your-access-key-id
aws_secret_access_key = your-secret-access-key
```

Replace `your-access-key-id` and `your-secret-access-key` with your actual AWS credentials.

### 3. AWS SSO Login

Authenticate with AWS using Single Sign-On (SSO):

```bash
AWS_PROFILE=space2stats aws sso login
```

This command will prompt you to log in through the browser and establish your session for the deployment.

### 4. Environment Variables Configuration

Before deploying, you need to set up the environment variables required by the CDK and the application.

#### Step 1: CDK Environment Configuration

Create a file named `aws_deployment.env` and define the following environment variables:

```bash
CDK_DEFAULT_ACCOUNT=your-account-id
CDK_DEFAULT_REGION=us-east-1
CDK_CERTIFICATE_ARN=your-certificate-arn
CDK_DOMAIN_NAME=space2stats.ds.io
```

#### Step 2: Application Environment Configuration

Create a file named `aws_app.env` and define the following environment variables:

```bash
DB_HOST=your-db-host
DB_PORT=your-db-port
DB_NAME=your-db-name
DB_USER=your-db-user
DB_PASSWORD=your-db-password
DB_TABLE_NAME=space2stats
```

### 5. CDK Deployment

Finally, deploy the API using the AWS CDK:

```bash
AWS_PROFILE=space2stats cdk deploy
```

### 6. Verification

Verify that the docs are accessible: `https://space2stats.ds.io/docs`

Follow the example in [`notebooks/space2stats_api_demo.ipynb`](notebooks/space2stats_api_demo.ipynb) 