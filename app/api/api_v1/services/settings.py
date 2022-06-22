import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)


TMP_FOLDER = os.environ.get('TMP_FOLDER', '.')
AWS_S3_BUCKET_NAME = os.environ.get('AWS_S3_BUCKET_NAME', 'poc-gio')
AWS_S3_PATH = os.environ.get('AWS_S3_PATH', 'historic-upload')
