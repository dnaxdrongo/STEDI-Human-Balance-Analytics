import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def spark_sql_query(glue_context: GlueContext, query: str, mapping: dict, transformation_ctx: str) -> DynamicFrame:
    spark = glue_context.spark_session
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    df = spark.sql(query)
    return DynamicFrame.fromDF(df, glue_context, transformation_ctx)


DATABASE = "stedi_db"

ACCEL_TABLE = "accelerometer_landing"
CUSTOMER_TRUSTED_TABLE = "customer_trusted"

TARGET_S3_PATH = "s3://stedi-dlbadley-2026/accelerometer/trusted/"
TARGET_TABLE = "accelerometer_trusted"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

accelerometer_landing_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=ACCEL_TABLE,
    transformation_ctx="accelerometer_landing_dyf"
)

customer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=CUSTOMER_TRUSTED_TABLE,
    transformation_ctx="customer_trusted_dyf"
)

# Rubric join: accelerometer_landing.user = customer_trusted.email
# Output: ONLY accelerometer columns
join_sql = """
SELECT
  a.user,
  a.timestamp,
  a.x,
  a.y,
  a.z
FROM accelerometer_landing a
WHERE a.user IS NOT NULL
  AND a.timestamp IS NOT NULL
  AND EXISTS (
    SELECT 1
    FROM customer_trusted c
    WHERE lower(c.email) = lower(a.user)
      AND c.shareWithResearchAsOfDate IS NOT NULL
  )
"""


accelerometer_trusted_dyf = spark_sql_query(
    glue_context=glueContext,
    query=join_sql,
    mapping={
        "accelerometer_landing": accelerometer_landing_dyf,
        "customer_trusted": customer_trusted_dyf
    },
    transformation_ctx="accelerometer_trusted_dyf"
)

glueContext.purge_s3_path(TARGET_S3_PATH, options={"retentionPeriod": 0})


sink = glueContext.getSink(
    path=TARGET_S3_PATH,
    connection_type="s3",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    transformation_ctx="accelerometer_trusted_sink"
)

sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=TARGET_TABLE)
sink.setFormat("json")
sink.writeFrame(accelerometer_trusted_dyf)

job.commit()
