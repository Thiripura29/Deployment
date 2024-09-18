import argparse
import traceback

from common import get_configs


def process_arguments():
    parser = argparse.ArgumentParser(
        description=(
            ""
        )
    )
    parser.add_argument("--project_path", type=str, required=True, help="project path")
    parser.add_argument("--env", type=str, required=True, help="environment")
    parser.add_argument("--package_name", type=str, required=True, help="name of the package")
    return parser.parse_args()


def get_entry_point_script(package_name, entry_point_name, config_path):
    return """
import argparse
import importlib
import os

import pkg_resources
from mlops.dq_processors.dq_loader import DQLoader
from mlops.factory.config_manager import ConfigurationManager
from mlops.factory.input_dataframe_manager import InputDataFrameManager
from mlops.factory.output_dataframe_manager import OutputDataFrameManager
from mlops.utils.common import set_env_variables
from mlops.utils.spark_manager import PysparkSessionManager

TYPE_MAPPING = {
    'str': str,
    'int': int,
    'float': float,
    'bool': bool
}


def generate_args_code(entry_points_config):
    parser = argparse.ArgumentParser(description="Entry point arguments")

    for param in entry_points_config['parameters']:
        name = f"--{param['name']}"
        param_type = param['type']
        required = param['required']
        help_text = param['help']
        choices = param.get('choices')

        if param_type not in TYPE_MAPPING:
            raise ValueError(f"Unsupported type: {param_type}")

        parser.add_argument(name, type=TYPE_MAPPING[param_type], required=required, help=help_text, choices=choices)

    return parser.parse_args()


def get_spark_session(config, platform):
    return PysparkSessionManager.start_session(config=config, platform=platform)


def get_data_sources_dfs(spark, entry_point_config):
    return InputDataFrameManager(spark, entry_point_config).create_dataframes().get_dataframes()


def load_and_execute_function(entry_point_config, input_df_list, config):
    function_path = entry_point_config.get("transformation_function_path")
    package_name = config.get('package_name')
    module_name, function_name = function_path.rsplit(".", 1)
    module = importlib.import_module(f"{package_name}.entry_points.{module_name}")
    function = getattr(module, function_name)
    return function(input_df_list, config)


def write_data_to_sinks(spark, output_df_dict, entry_point_config):
    OutputDataFrameManager(spark, output_df_dict, entry_point_config).write_data_to_sinks()


def handle_entry_point(entry_point_config, config):
    entry_point_type = entry_point_config.get('type')
    if entry_point_type == "table-manager":
        load_and_execute_function(entry_point_config, None, config)
    elif entry_point_type == "source-sink":
        transformed_df_dict = {}
        dq_dfs_dict = {}
        spark_config = entry_point_config.get('spark_configs', {})
        platform = config.get('platform')
        spark = get_spark_session(spark_config, platform)
        input_df_dict = get_data_sources_dfs(spark, entry_point_config)
        transform_function = entry_point_config.get('transformation_function_path')
        if transform_function:
            transformed_df_dict = load_and_execute_function(entry_point_config, input_df_dict, config)

        transformed_df_dict = {**transformed_df_dict, **input_df_dict}

        dq_specs = entry_point_config.get('dq_specs')
        if dq_specs:
            dq_dfs_dict = execute_df_specs(spark, transformed_df_dict, entry_point_config)

        dq_dfs_dict = {**transformed_df_dict, **input_df_dict, **dq_dfs_dict}

        write_data_to_sinks(spark, dq_dfs_dict, entry_point_config)
    else:
        raise ValueError(f"Unsupported entry point type: {entry_point_type}")


def execute_df_specs(spark, df_dict, config):
    dq_loader = DQLoader(config)
    return dq_loader.process_dq(spark, df_dict)


def main():
    entry_point = '$replace_entry_point_name'
    
    config_path = pkg_resources.resource_filename('$package_name', '$config_path')
    config_manager = ConfigurationManager(config_path)
    config = config_manager.get_config_as_json()
    entry_point_config = config_manager.get_entry_point_config(entry_point)

    if 'env_variables' in entry_point_config:
        set_env_variables(entry_point_config['env_variables'])

    handle_entry_point(entry_point_config, config)


if __name__ == '__main__':
    main()

""".replace('$replace_entry_point_name', entry_point_name).replace('$config_path', config_path).replace('$package_name',
                                                                                                        package_name)


def generate_setup_py_script(package_name: str, version: str, scripts_py_entry_points):
    console_scripts_str = "[" + ",\n\t\t\t\t\t\t\t".join(
        [f"'{entry_point['entry_point_path']}'" for entry_point in scripts_py_entry_points]) + "]"
    return """
from setuptools import setup, find_namespace_packages

requirements = []
with open('requirements.txt', 'r') as file:
    for line in file:
        line = line.strip()
        requirements.append(line.strip())

# Minimal example for versioning purposes, not ready yet.
setup(
    name="$package_name",
    version="$version",
    packages=find_namespace_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[requirements],
    python_requires=">=3.7",
    include_package_data=True,
    entry_points={
        "console_scripts": $replace_console_scripts
    }
)
    """.replace('$replace_console_scripts', console_scripts_str).replace('$package_name', package_name).replace(
        '$version', str(version))


def generate_write_entry_point_scripts(configs, project_path, entry_point_folder, configs_file_path):
    scripts_py_entry_points = []
    entry_points = configs.get("entry_points", None)
    for entry_point in entry_points:
        entry_point_keys = list(entry_point.keys())
        entry_point_name = entry_point_keys[0]
        entry_point_path = f"{project_path}{entry_point_folder}/{entry_point_name}"
        entry_point_script = get_entry_point_script(configs['package_name'], entry_point_name, configs_file_path)
        entry_point_script_file_name = f"entry_point_{entry_point_name}.py"
        entry_point_script_file_path = f"{entry_point_path}/{entry_point_script_file_name}"
        with open(entry_point_script_file_path, 'w') as f:
            f.write(entry_point_script)
            scripts_py_entry_points.append({"entry_point_name": entry_point_name,
                                            "entry_point_path": f"{entry_point_name}={configs['package_name']}.entry_points.{entry_point_name}"
                                                                f".entry_point_{entry_point_name}:main",
                                            "entry_point_script_file_name": entry_point_script_file_name,
                                            "entry_point_script_file_path": entry_point_script_file_path})
    return scripts_py_entry_points


def write_setup_py_scripts(setup_py_file_path, setup_py_script):
    with open(setup_py_file_path, 'w') as f:
        f.write(setup_py_script)


def write_manifest_in_scripts(manifest_in_file_path, manifest_in_content):
    with open(manifest_in_file_path, 'w') as f:
        f.write(manifest_in_content)


def main():
    try:
        args = process_arguments()
        project_path = args.project_path
        package_name = args.package_name
        env = args.env
        relative_config_path = f"configs/{env}.yaml"
        configs_file_path = f"{project_path}/src/{package_name}/{relative_config_path}"
        entry_point_folder = f"/src/{package_name}/entry_points"
        setup_py_file_path = f"{project_path}/setup.py"
        manifest_file_path = f"{project_path}/MANIFEST.in"
        configs = get_configs(configs_file_path)
        print(configs)
        scripts_py_entry_points = generate_write_entry_point_scripts(configs, project_path, entry_point_folder,
                                                                     relative_config_path)
        setup_py_script = generate_setup_py_script(package_name=configs['package_name'],
                                                   version=configs['package_version'],
                                                   scripts_py_entry_points=scripts_py_entry_points)
        write_setup_py_scripts(setup_py_file_path, setup_py_script)
        write_manifest_in_scripts(manifest_file_path, "graft src")
    except Exception as ex:
        print(traceback.format_exc())
        raise ex


if __name__ == '__main__':
    main()
