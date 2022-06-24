import base64
import json
import logging
import os

import boto3
import psycopg2
from botocore.exceptions import ClientError

DB_CREDENTIALS = os.environ['DB_CREDENTIALS']
DB_REGION = os.environ['DB_REGION']

JOB_NAME = os.environ['RESTORE_JOB_NAME']

sm_session = boto3.session.Session()
s3_client = boto3.client('s3')
sns = boto3.client('sns')
glue_client = boto3.client('glue')

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

    connection = psycopg2.connect(user=secrets['username'],
                                  password=secrets['password'],
                                  host=secrets['host'],
                                  port=secrets['port'],
                                  database=secrets['dbClusterIdentifier'])
    connection.autocommit = True
    return connection


def truncate_table(connection, table_name):
    cursor = connection.cursor()

    # executing the sql statement
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    connection.commit()

    # closing connection
    connection.close()


def start_job_run_to_restore_table(table_name, s3_bu_path):
    glue_client.start_job_run(
        JobName=JOB_NAME,
        Arguments={
            '--TABLE_NAME': table_name,
            '--S3_INPUT_PATH': s3_bu_path
        }
    )


def lambda_handler(event, context):
    table_name = event['table_name']
    s3_bu_path = event['s3_bu_path']

    connection = connection_to_rds()
    truncate_table(connection, table_name)
    start_job_run_to_restore_table(table_name, s3_bu_path)
