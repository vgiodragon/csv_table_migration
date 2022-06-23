import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'CATALOG_DATABASE_NAME',
    'TABLE_NAME',
    'OUTPUT_BUCKET',
    'OUTPUT_PATH'
])

################################################
#                   VARIABLES
################################################

CATALOG_DATABASE_NAME = args['CATALOG_DATABASE_NAME']
TABLE_NAME = args['TABLE_NAME']
OUTPUT_BUCKET = args['OUTPUT_BUCKET']
OUTPUT_PATH = args['OUTPUT_PATH']

datetime_now = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database=CATALOG_DATABASE_NAME,
    table_name=f'postgres_public_{TABLE_NAME}',

)

glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame.coalesce(1),
    connection_type="s3",
    format="avro",
    connection_options={"path": f"s3://{OUTPUT_BUCKET}/{OUTPUT_PATH}/{TABLE_NAME}/{datetime_now}"},
)
