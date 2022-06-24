import base64
import json

import boto3
from botocore.exceptions import ClientError


class S3Service(object):

    def __init__(self, bucket_name, path=None):
        self.bucket_name = bucket_name
        self.path = path

    def build_key_name(self, table_name, *extra_object_names):
        return "/".join(map(str, filter(None, [self.path, table_name, *extra_object_names])))

    def upload_file_from_path(self, local_path_filename, s3_path_key):
        s3 = boto3.client('s3')
        s3.upload_file(local_path_filename, self.bucket_name, s3_path_key)


class SecretManagerService(object):

    def __init__(self, secret_name, region_name):
        self.secret_name = secret_name
        self.region_name = region_name

    def get_secret(self):

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=self.region_name
        )

        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=self.secret_name
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
