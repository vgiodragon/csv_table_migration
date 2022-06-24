import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)


TMP_FOLDER = os.environ.get('TMP_FOLDER', '.')
AWS_S3_BUCKET_NAME = os.environ.get('AWS_S3_BUCKET_NAME', 'poc-gio')
AWS_S3_PATH = os.environ.get('AWS_S3_PATH', 'historic-upload')
DB_CREDENTIALS = os.environ.get('DB_CREDENTIALS', 'poc/historic-date/aurora-postgres')
DB_REGION = os.environ.get('DB_REGION', 'us-east-2')
