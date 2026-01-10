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

STEP_LANDING_TABLE = "step_trainer_landing"
CUSTOMER_CURATED_TABLE = "customer_curated"

TARGET_S3_PATH = "s3://stedi-dlbadley-2026/step_trainer/trusted/"
TARGET_TABLE = "step_trainer_trusted"


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------
# Sources: Data Catalog tables
# -------------------------
step_landing_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=STEP_LANDING_TABLE,
    transformation_ctx="step_landing_dyf"
)

customer_curated_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=CUSTOMER_CURATED_TABLE,
    transformation_ctx="customer_curated_dyf"
)

# -------------------------
# Transform: semi-join filter by serialNumber existence
# Keeps ONLY step_trainer columns; avoids join duplication/oddities.
# -------------------------
filter_sql = """
SELECT s.*
FROM step_trainer_landing s
WHERE EXISTS (
  SELECT 1
  FROM customer_curated c
  WHERE lower(c.serialNumber) = lower(s.serialNumber)
)
"""

step_trusted_dyf = spark_sql_query(
    glue_context=glueContext,
    query=filter_sql,
    mapping={
        "step_trainer_landing": step_landing_dyf,
        "customer_curated": customer_curated_dyf
    },
    transformation_ctx="step_trusted_dyf"
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
    transformation_ctx="step_trusted_sink"
)

sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=TARGET_TABLE)
sink.setFormat("json")
sink.writeFrame(step_trusted_dyf)

job.commit()
