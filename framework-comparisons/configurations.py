import yaml

with open('configurations.yaml', 'r') as config_file:
    configs = yaml.safe_load(config_file)

kafka_topic = configs.get('kafka_topic', 'default_topic')
number_of_elements_to_send = configs.get('number_of_elements_to_send', 0)
chance_of_missing_value = configs.get('chance_of_missing_value', 0.0)
sleep_seconds_between_messages = configs.get('sleep_seconds_between_messages', 0.0)