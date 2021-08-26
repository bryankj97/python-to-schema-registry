from confluent_kafka import schema_registry
import json



client.get_versions('dbserver1.inventory.customers-key')
schema_instance = client.get_latest_version('dbserver1.inventory.customers-key')
# Obtain schema id
schema_instance.schema_id

subject = 'dbserver1.inventory.customers-value'


def create_client(url='http://localhost:8081'):
    params = {'url': url}
    client = schema_registry.SchemaRegistryClient(params)
    return client


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


def update_schema_registry(subject, schema_content):
    client = 


client.get_schema(6).schema_str
client.get_schema(15).schema_str

schema = json.loads(client.get_schema(15).schema_str)
schema['fields'][0]['type'][1]['fields'].append({'name': 'number', 'type': ['null', 'int'], 'default': None})
new_schema_str = str(schema)
new_schema_str = new_schema_str.replace(" ", "")
new_schema_str = new_schema_str.replace("'", "")



schema['fields'][1]['type'][1] = {'type': 'record', 'name': 'Value', 'fields': [{'name': 'address', 'type': 'string'}], 'connect.name': 'dbserver1.inventory.customers.Value'}

schema_str = str(schema)

client.get_schema(6).references


new_schema = schema_registry.Schema(new_schema_str, 'AVRO')

new_schema

client.register_schema('dbserver1.inventory.customers-value', new_schema)


client.test_compatibility('dbserver1.inventory.customers-value', new_schema, version=1)


new_schema.schema_str
client.get_schema(15).schema_str