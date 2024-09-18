import argparse
import os
import json
import traceback

import requests
from jinja2 import Environment, FileSystemLoader, TemplateNotFound

# Replace these variables with your information
from common import get_configs, render_template, deploy_job, get_job_template, get_job_config

DOMAIN = 'https://dbc-63695d69-a8da.cloud.databricks.com/'
TOKEN = 'dapi25520dd420a79ec9822c37c31e08d3d5'


def process_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Used to deploy  jobs to databricks. It takes two arguments --templates_path: path where job jinja "
            "template exists --template_name name of the template to process"
        )
    )

    parser.add_argument("--templates_path", type=str, required=True, help="template path")
    parser.add_argument("--template_name", type=str, required=True, help="template name")
    parser.add_argument("--config_path", type=str, required=True, help="config path")

    return parser.parse_args()


try:
    args = process_arguments()

    # Define the path to the directory containing the template
    template_dir = args.templates_path  # Directory path containing the template file
    template_file = args.template_name  # Template file name
    config_path = args.config_path

    # Check if the template directory exists
    template = get_job_template(template_dir, template_file)

    template_vars = get_configs(config_path)
    # Render the Jinja template with the configuration variables
    job_config = render_template(template_vars, template)
    print(job_config)
    # Convert the rendered JSON template to a dictionary
    job_config_dict = get_job_config(job_config)

    print(job_config_dict)
    deploy_job(job_config_dict, DOMAIN, TOKEN)

except Exception as e:
    print(traceback.format_exc())
    raise e
