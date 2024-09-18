import boto3
import json
import requests
import time

# Initialize a session using Amazon IAM
session = boto3.Session(
    aws_access_key_id='YOUR_AWS_ACCESS_KEY',
    aws_secret_access_key='YOUR_AWS_SECRET_KEY',
    region_name='us-west-2'  # Specify your region
)

# Create CloudFormation client
cf_client = session.client('cloudformation')

# CloudFormation template
cf_template = """

"""

# Parameters for the stack
stack_name = 'DatabricksDynamoDBInstanceProfileStack'

try:
    # Create the CloudFormation stack
    response = cf_client.create_stack(
        StackName=stack_name,
        TemplateBody=cf_template,
        Capabilities=['CAPABILITY_NAMED_IAM'],
        OnFailure='DELETE'
    )

    # Wait until the stack status changes
    print(f"Creating stack {stack_name}...")
    waiter = cf_client.get_waiter('stack_create_complete')
    waiter.wait(StackName=stack_name)
    print(f"Stack {stack_name} created successfully.")

    # Get the output value (Instance Profile ARN)
    response = cf_client.describe_stacks(StackName=stack_name)
    outputs = response['Stacks'][0]['Outputs']
    instance_profile_arn = None
    for output in outputs:
        if output['OutputKey'] == 'InstanceProfileArn':
            instance_profile_arn = output['OutputValue']
            print(f"Instance Profile ARN: {instance_profile_arn}")

    # Databricks Configuration
    databricks_instance = 'https://<databricks-instance>.cloud.databricks.com'  # Replace with your Databricks instance URL
    databricks_token = 'YOUR_DATABRICKS_TOKEN'  # Replace with your Databricks personal access token
    headers = {
        'Authorization': f'Bearer {databricks_token}',
        'Content-Type': 'application/json'
    }

    # Attach the instance profile to the Databricks workspace
    if instance_profile_arn:
        instance_profile_payload = {
            "instance_profile_arn": instance_profile_arn
        }
        response = requests.post(
            f'{databricks_instance}/api/2.0/instance-profiles/add',
            headers=headers,
            data=json.dumps(instance_profile_payload)
        )

        if response.status_code == 200:
            print("Instance profile successfully added to Databricks workspace.")
        else:
            print(f"Error adding instance profile to Databricks: {response.text}")

except Exception as e:
    print(f"Error: {str(e)}")
