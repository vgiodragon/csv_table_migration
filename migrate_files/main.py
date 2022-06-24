import base64
import json
import logging
import os
from urllib.parse import unquote_plus

import boto3
import psycopg2
from botocore.exceptions import ClientError

DB_CREDENTIALS = os.environ['DB_CREDENTIALS']
DB_REGION = os.environ['DB_REGION']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

sm_session = boto3.session.Session()
s3_client = boto3.client('s3')
sns = boto3.client('sns')

# Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_secret():

    secret_name = DB_CREDENTIALS
    region_name = DB_REGION

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            secret = base64.b64decode(
                get_secret_value_response['SecretBinary'])
            return json.loads(secret)


def connection_to_rds():
    secrets = get_secret()
    print(secrets)
    connection = psycopg2.connect(user=secrets['username'],
                                  password=secrets['password'],
                                  host=secrets['host'],
                                  port=secrets['port'],
                                  database=secrets['dbClusterIdentifier'])
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
    elif table == 'jobs':
        return (int(elements[0]), elements[1])
    elif table == 'hired_employees':
        return (int(elements[0]), elements[1], elements[2], int(elements[3]), int(elements[4]))


def get_list_values_to_insert(csv_file, table):
    values = []
    for row in csv_file.split('\n'):
        if row:
            values.append(get_value_by_table(row, table))
    return values


def get_str_n_element(table):
    if table == 'departments':
        return "(%s,%s)"
    elif table == 'jobs':
        return "(%s,%s)"
    elif table == 'hired_employees':
        return "(%s,%s,%s,%s,%s)"


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
    try:
        logging.info(event)
        item = parse_s3_event(event)
        csv_file = get_csv_file(item)
        values = get_list_values_to_insert(csv_file, item['table'])
        connection = connection_to_rds()
        insert_values_to_db(connection, values, item['table'])
        message = f"Successful insertion."
        final_status = '(SUCCESS)'
    except Exception as e:
        message = f"Insertion error {e}"
        final_status = '(FAILED)'
    sns.publish(TopicArn=SNS_TOPIC_ARN, Message=message, Subject=f'{final_status} MODAMA: Datalake File Validator')
