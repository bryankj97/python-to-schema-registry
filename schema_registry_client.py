from confluent_kafka import schema_registry
import argparse
import json

def create_client(url):
    params = {'url': url}
    client = schema_registry.SchemaRegistryClient(params)
    return client

def get_schema_object(client, subject):
    schema_id = client.get_latest_version(subject).schema_id
    return client.get_schema(schema_id)

def register_schema_from_json(client, schema_json, subject):
    schema_str = json.dumps(schema_json)
    schema_str = schema_str.replace(" ", "")
    schema = schema_registry.Schema(schema_str, 'AVRO')
    client.register_schema(subject, schema)

def update_value(client, value_subject, analytics_subject):
    # get latest value Schema object
    value_schema = get_schema_object(client, value_subject)
    # get latest analytics Schema object
    analytics_schema = get_schema_object(client, analytics_subject)

    # compare fields
    value_schema_str = value_schema.schema_str
    value_schema_json = json.loads(value_schema_str)
    analytics_schema_str = analytics_schema.schema_str
    analytics_schema_json = json.loads(analytics_schema_str)

    # if fields are equal, no need to update. else update with analytics
    if value_schema_json['fields'] == analytics_schema_json['fields']:
        print("Schemas up to date")
        return
    # update value fields
    value_schema_json['fields'] = analytics_schema_json['fields']

    # create new value schema
    register_schema_from_json(client, value_schema_json, value_subject)


def add_analytics(client, value_subject, analytics_subject):
    # get latest value Schema object
    value_schema = get_schema_object(client, value_subject)
    value_schema_str = value_schema.schema_str

    # modify for analytics schema
    schema_json = json.loads(value_schema_str)
    schema_json['connect.name'] = schema_json['connect.name'].replace("Value", "Analytics")
    
    # create new analytics schema
    register_schema_from_json(client, schema_json, analytics_subject)

def run(target_subject, registry_url='http://localhost:8081'):
    client = create_client(registry_url)
    value_subject = f'{target_subject}-value'
    analytics_subject = f'{target_subject}-analytics'
    available_subjects = client.get_subjects()
    if value_subject not in available_subjects:
        print("Value subject missing... Check downstream...")
        return
    if analytics_subject in available_subjects:
        update_value(client, value_subject, analytics_subject)
        return
    else:
        add_analytics(client, value_subject, analytics_subject)
        return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry_url", type=str)
    parser.add_argument("--logical_name", type=str)
    parser.add_argument("--database", type=str)
    parser.add_argument("--table", type=str)
    args = parser.parse_args()
    target_subject= f'{args.logical_name}.{args.database}.{args.table}'
    run(target_subject, args.registry_url)