import importlib
import argparse
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


def get_spark_session():
    return PysparkSessionManager.start_session()


def get_data_sources_dfs(spark, entry_point_config):
    return InputDataFrameManager(spark, entry_point_config).create_dataframes().get_dataframes()


def load_and_execute_function(entry_point_config, input_df_list, config):
    function_path = entry_point_config.get("function_path")
    module_name, function_name = function_path.rsplit(".", 1)
    module = importlib.import_module(f"entry_points.{module_name}")
    function = getattr(module, function_name)
    return function(input_df_list, config)


def write_data_to_sinks(spark, output_df_dict, entry_point_config):
    OutputDataFrameManager(spark, output_df_dict, entry_point_config).write_data_to_sinks()


def handle_entry_point(entry_point_config, config):
    entry_point_type = entry_point_config.get('type')
    if entry_point_type == "table-manager":
        load_and_execute_function(entry_point_config, None, config)
    elif entry_point_type == "source-sink":
        spark = get_spark_session()
        input_df_list = get_data_sources_dfs(spark, entry_point_config)
        output_df_dict = load_and_execute_function(entry_point_config, input_df_list, config)
        write_data_to_sinks(spark, output_df_dict, entry_point_config)
    else:
        raise ValueError(f"Unsupported entry point type: {entry_point_type}")


def main():
    entry_point = 'preprocessing'

    config_manager = ConfigurationManager('C:/Users/Prudhvi_Akella/PycharmProjects/mlops_with_mlflow/configs/dev.yaml')
    config = config_manager.get_config()
    entry_point_config = config_manager.get_entry_point_config(entry_point)

    if 'env_variables' in entry_point_config:
        set_env_variables(entry_point_config['env_variables'])

    handle_entry_point(entry_point_config, config)


if __name__ == '__main__':
    main()
