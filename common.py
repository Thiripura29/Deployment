import json
import os

import requests
from jinja2 import FileSystemLoader, Environment, TemplateNotFound
from mlops.factory.config_manager import ConfigurationManager


def get_configs(config_path: str):
    # Load the configuration from the specified YAML file using ConfigurationManager
    config = ConfigurationManager(config_path).get_config()

    # Convert the configuration to a dictionary format suitable for Jinja template rendering
    configs = config.get_config_as_json()
    return configs


def get_job_template(template_dir, template_file):
    # Check if the template directory exists
    if not os.path.isdir(template_dir):
        print(f"Template directory '{template_dir}' does not exist.")
    else:
        print(f"Template directory '{template_dir}' found.")

        # Load the Jinja2 environment with the specified template directory
        file_loader = FileSystemLoader(template_dir)
        env = Environment(loader=file_loader)

        # Attempt to load the template file
        try:
            template = env.get_template(template_file)
            print(f"Template '{template_file}' loaded successfully.")
            return template
        except TemplateNotFound:
            print(f"Template '{template_file}' not found in directory '{template_dir}'.")
            return None


def get_job_config(job_config: str):
    try:
        job_config_dict = json.loads(job_config)
        print("Rendered job configuration:", job_config_dict)
        return job_config_dict
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None


def render_template(template_vars, template):
    try:
        job_config = template.render(template_vars)
        print("Template rendered successfully.")
        return job_config
    except Exception as e:
        print(f"Error rendering template: {e}")
        return None


def deploy_job(job_config: str, DOMAIN: str, TOKEN: str):
    endpoint = f'{DOMAIN}/api/2.0/jobs/create'
    headers = {
        'Authorization': f'Bearer {TOKEN}',
        'Content-Type': 'application/json'
    }
    # Make the POST request
    response = requests.post(endpoint, headers=headers, data=json.dumps(job_config))

    # Check the response
    if response.status_code == 200:
        job_info = response.json()
        print(json.dumps(job_info, indent=4))
    else:
        print(f"Error {response.status_code}: {response.text}")
