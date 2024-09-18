import argparse

import os
from pathlib import Path
from urllib.parse import urlparse

from jinja2 import Template
from mlops.utils.storage.s3 import S3Storage

from common import get_configs, render_template, get_job_template, deploy_job, get_job_config

DOMAIN = 'https://dbc-f5cb4ef1-bc75.cloud.databricks.com/'
TOKEN = 'dapia095b4e0b20607855670c0ce3e7473f6'


def get_sql_files_recursive(directory):
    sql_files = [file.as_posix() for file in Path(directory).rglob('*.sql')]
    return sql_files


def get_file_content(sql_file_path: str) -> str:
    with open(sql_file_path, 'r') as f:
        return f.read()


def get_s3_file_url(s3_artefact_path, file_path):
    return s3_artefact_path + file_path.split('metadata')[1]


def process_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Used to deploy  jobs to databricks. It takes two arguments --templates_path: path where job jinja "
            "template exists --template_name name of the template to process"
        )
    )

    parser.add_argument("--assets_path", type=str, required=True, help="template path")
    parser.add_argument("--config_path", type=str, required=True, help="config path")
    parser.add_argument("--templates_path", type=str, required=True, help="template path")
    parser.add_argument("--template_name", type=str, required=True, help="template name")

    return parser.parse_args()


args = process_arguments()
config_path = args.config_path
assets_path = args.assets_path
template_dir = args.templates_path
template_file = args.template_name

sql_files = get_sql_files_recursive(assets_path)
sql_s3_paths = []
template_vars = get_configs(config_path)
if len(sql_files) > 0:
    for sql_file_path in sql_files:
        content = get_file_content(sql_file_path)
        rendered_content = render_template(template_vars, Template(content))
        s3_artefact_path = template_vars.get('bucket')
        s3_sql_file_path = get_s3_file_url(s3_artefact_path + '/artefacts', sql_file_path)
        sql_s3_paths.append(s3_sql_file_path)
        S3Storage.write_payload_to_file(urlparse(s3_sql_file_path), rendered_content)

    template_vars['jobs']["sql_assets_paths"] = ",".join(sql_s3_paths)
    template = get_job_template(template_dir, template_file)
    job_config = render_template(template_vars, template)
    job_config_dict = get_job_config(job_config)
    deploy_job(job_config_dict, DOMAIN, TOKEN)
