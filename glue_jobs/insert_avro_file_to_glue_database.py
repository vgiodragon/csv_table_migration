import sys

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'CATALOG_DATABASE_NAME',
    'TABLE_NAME',
    'S3_INPUT_PATH'
])

################################################
#                   VARIABLES
################################################

CATALOG_DATABASE_NAME = args['CATALOG_DATABASE_NAME']
TABLE_NAME = args['TABLE_NAME']
S3_INPUT_PATH = args['S3_INPUT_PATH']


def apply_mapping_for_hired_hired_employees(dynamic_frame):
    return ApplyMapping.apply(
        frame=dynamic_frame,
        mappings=[
            ("id", "long", "id", "long"),
            ("name", "string", "name", "string"),
            ("datetime", "string", "datetime", "timestamp"),
            ("department_id", "long", "department_id", "long"),
            ("job_id", "long", "job_id", "long"),
        ],
    )


# Script generated for node Amazon S3
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="avro",
    connection_options={
        "paths": [S3_INPUT_PATH],
        "recurse": True,
    },
)

if TABLE_NAME == 'hired_employees':
    dynamic_frame = apply_mapping_for_hired_hired_employees(dynamic_frame)

# Script generated for node PostgreSQL
glueContext.write_dynamic_frame.from_catalog(
    frame=dynamic_frame,
    database=CATALOG_DATABASE_NAME,
    table_name=f'postgres_public_{TABLE_NAME}',
)
