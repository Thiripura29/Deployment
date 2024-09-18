import requests
import json




# Databricks instance URL and personal access token
databricks_instance = "https://<databricks-instance>"
personal_access_token = "<your-personal-access-token>"

# Define the cluster policy for job clusters with min and max workers
policy_definition = {
    "name": "Job Cluster Policy with Unity Catalog and Worker Limits",
    "definition": {
        "cluster_type": {
            "type": "fixed",
            "value": "job"
        },
        "spark_version": {
            "type": "allowlist",
            "values": ["10.4.x-cpu-ml-scala2.12"]
        },
        "autotermination_minutes": {
            "type": "range",
            "minValue": 10,
            "maxValue": 60,
            "defaultValue": 30
        },
        "num_workers": {
            "type": "range",
            "minValue": 1,
            "maxValue": 10,
            "defaultValue": 2
        },
        "enable_unity_catalog": {
            "type": "fixed",
            "value": True
        },
        "unity_catalog_metastore_id": {
            "type": "fixed",
            "value": "your-metastore-id"
        },
        "aws_attributes": {
            "instance_profile_arn": {
                "type": "fixed",
                "value": "arn:aws:iam::123456789012:instance-profile/your-instance-profile"
            }
        }
    }
}

# API endpoint for creating the cluster policy
url = f"{databricks_instance}/api/2.0/policies/clusters/create"

# Headers for the request
headers = {
    "Authorization": f"Bearer {personal_access_token}",
    "Content-Type": "application/json"
}

# Send the request to create the cluster policy
response = requests.post(url, headers=headers, data=json.dumps(policy_definition))

# Check the response
if response.status_code == 200:
    print("Job cluster policy created successfully.")
    print(response.json())
else:
    print(f"Failed to create job cluster policy: {response.status_code}")
    print(response.text)
