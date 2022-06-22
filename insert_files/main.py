import base64
import json
import logging
import os
from urllib.parse import unquote_plus

import boto3
import psycopg2
from botocore.exceptions import ClientError

DB_CREDENTIALS = os.environ.get('DB_CREDENTIALS', 'poc/historic-date/postgres')
DB_REGION = os.environ.get('DB_REGION', 'us-east-2')

sm_session = boto3.session.Session()
s3_client = boto3.client('s3')

# Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_secret():

    secret_name = DB_CREDENTIALS
    region_name = DB_REGION

    # Create a Secrets Manager client
    client = sm_session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            secret = base64.b64decode(
                get_secret_value_response['SecretBinary'])
            return json.loads(secret)


def connection_to_rds():
    secrets = get_secret()
    connection = psycopg2.connect(user=secrets['username'],
                                  password=secrets['password'],
                                  host=secrets['host'],
                                  port=secrets['port'],
                                  database=secrets['dbInstanceIdentifier'])
    connection.autocommit = True
    return connection


def parse_s3_event(event):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'])
    table = key.split('/')[-2]
    return {
        'bucket': bucket,
        'key': key,
        'table': table
    }


def get_csv_file(item):
    csv_file_object = s3_client.get_object(Bucket=item['bucket'], Key=item['key'])
    return csv_file_object['Body'].read().decode('us-ascii')


def get_value_by_table(row, table):
    elements = row.split(',')
    if table == 'departments':
        return (int(elements[0]), elements[1])


def get_list_values_to_insert(csv_file, table):
    values = []
    for row in csv_file.split('\n'):
        values.append(get_value_by_table(row, table))
    return values


def get_str_n_element(table):
    if table == 'departments':
        return "(%s,%s)"


def insert_values_to_db(connection, values, table):
    cursor = connection.cursor()
    str_n_element = get_str_n_element(table)
    # cursor.mogrify() to insert multiple values
    args = ','.join(cursor.mogrify(str_n_element, i).decode('utf-8')
                    for i in values)

    # executing the sql statement
    cursor.execute(f"INSERT INTO {table} VALUES {args}")
    connection.commit()

    # closing connection
    connection.close()


def lambda_handler(event, context):
    item = parse_s3_event(event)
    csv_file = get_csv_file(item)
    values = get_list_values_to_insert(csv_file, item['table'])
    connection = connection_to_rds()
    insert_values_to_db(connection, values, item['table'])
