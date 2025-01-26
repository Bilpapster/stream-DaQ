import yaml
import os

# Get the directory where the current configuration file resides
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the full path to configurations.yaml
config_file_path = os.path.join(current_dir, 'configurations.yaml')

with open(config_file_path, 'r') as config_file:
    configs = yaml.safe_load(config_file)

# kafka configurations
kafka_topic = configs.get('kafka_topic', 'default_topic')
kafka_server = configs.get('kafka_server', 'localhost:9092')

# stream configurations
number_of_elements_to_send = int(float(configs.get('number_of_elements_to_send', 100)))
chance_of_missing_value = float(configs.get('chance_of_missing_value', 0.1))
sleep_seconds_between_messages = float(configs.get('sleep_seconds_between_messages', '1e-10'))
min_stream_value = int(configs.get('min_stream_value', 0))
max_stream_value = int(configs.get('max_stream_value', 100))

# data quality checks configurations
low_threshold = int(configs.get('low_threshold', 10))
high_threshold = int(configs.get('high_threshold', 100))

# results sink configurations
directory_base_name = configs.get('directory_base_name', './results/')
pathway_directory = configs.get('pathway_directory', 'pathway/')
faust_directory = configs.get('faust_directory', 'faust/')
bytewax_directory = configs.get('bytewax_directory', 'bytewax/')
quix_directory = configs.get('quix_directory', 'quix/')
missing_values_file_name = configs.get('missing_values_file_name', 'missing_values.csv')
out_of_range_values_file_name = configs.get('out_of_range_values_file_name', 'out_of_range_values.csv')
