import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def spark_sql_query(glue_context: GlueContext, query: str, mapping: dict, transformation_ctx: str) -> DynamicFrame:
    """
    Register each DynamicFrame as a temp view, run Spark SQL, return a DynamicFrame.
    """
    spark = glue_context.spark_session
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    df = spark.sql(query)
    return DynamicFrame.fromDF(df, glue_context, transformation_ctx)


# -------------------------
# CONFIG
# -------------------------
DATABASE = "stedi_db"

CUSTOMER_TRUSTED_TABLE = "customer_trusted"
ACCEL_TRUSTED_TABLE = "accelerometer_trusted"

TARGET_S3_PATH = "s3://stedi-dlbadley-2026/customer/curated/"
TARGET_TABLE = "customer_curated"


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------
# Sources: Data Catalog tables
# -------------------------
customer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=CUSTOMER_TRUSTED_TABLE,
    transformation_ctx="customer_trusted_dyf"
)

accelerometer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=ACCEL_TRUSTED_TABLE,
    transformation_ctx="accelerometer_trusted_dyf"
)

# -------------------------
# Transform: keep ONLY customers who have accelerometer data
# Output: ONLY customer columns
# -------------------------
curated_sql = """
SELECT DISTINCT c.*
FROM customer_trusted c
JOIN accelerometer_trusted a
  ON lower(c.email) = lower(a.user)
"""

customer_curated_dyf = spark_sql_query(
    glue_context=glueContext,
    query=curated_sql,
    mapping={
        "customer_trusted": customer_trusted_dyf,
        "accelerometer_trusted": accelerometer_trusted_dyf
    },
    transformation_ctx="customer_curated_dyf"
)

# -------------------------
# Target: S3 + Catalog auto-create/update
# -------------------------
glueContext.purge_s3_path(TARGET_S3_PATH, options={"retentionPeriod": 0})

sink = glueContext.getSink(
    path=TARGET_S3_PATH,
    connection_type="s3",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    transformation_ctx="customer_curated_sink"
)

sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=TARGET_TABLE)

# Keep JSON to match course examples. (You *can* switch to parquet later.)
sink.setFormat("json")

sink.writeFrame(customer_curated_dyf)
job.commit()
