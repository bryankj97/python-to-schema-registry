from confluent_kafka import schema_registry
import json

subject = 'dbserver1.inventory.customers-value'

def create_client(url='http://localhost:8081'):
    params = {'url': url}
    client = schema_registry.SchemaRegistryClient(params)
    return client

def get_versions(client, subject='dbserver1.inventory.customers-value'):
    return client.get_versions(subject)

def get_version(client, subject='dbserver1.inventory.customers-value', version=None):
    if not version:
        return client.get_latest_version(subject)
    else:
        return client.get_version(subject, version)

def check_existence(client, subject, schema_content):
    available_schemas = client.get_versions(subject)
    latest_schema_id = 0
    for version_id in available_schemas:
        schema_id = client.get_version(subject, version_id).schema_id
        schema = json.loads(client.get_schema(schema_id).schema_str)
        if schema['fields'] == schema_content:
            print("Current schema already registered")
            return None
        if schema['fields'] in schema_content:
            # we collect latest schema that is a subset of schema_content
            latest_schema_id = schema_id
    return latest_schema_id


def update_schema_registry(client, subject, schema_content):
    # check if schema already exists
    old_schema_id = check_existence(client, subject, schema_content)
    if not old_schema_id:
        return None
    old_schema = client.get_schema(old_schema_id)
    old_schema_str = old_schema.schema_str
    new_schema_str = json.loads(old_schema_str)
    # TO-DO:
    # get updated column from new_schema_str

    # Example:
    new_schema_str['fields'].append({'name': 'phone', 'type': ['null', 'int'], 'default': None})
    new_schema_str = json.dumps(new_schema_str)
    new_schema_str = new_schema_str.replace(" ", "")
    new_schema = schema_registry.Schema(new_schema_str, 'AVRO')

    # test compataibility
    client.test_compatibility('dbserver1.inventory.customers-value', new_schema)

    #register schema
    client.register_schema('dbserver1.inventory.customers-value', new_schema)
