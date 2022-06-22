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
