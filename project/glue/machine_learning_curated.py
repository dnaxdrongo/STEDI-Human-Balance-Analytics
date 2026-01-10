import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def spark_sql_query(glue_context: GlueContext, query: str, mapping: dict, transformation_ctx: str) -> DynamicFrame:
    """
    Registers each DynamicFrame as a temp view (name = dict key), then runs Spark SQL.
    Returns a DynamicFrame.
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

STEP_TRAINER_TRUSTED_TABLE = "step_trainer_trusted"
ACCEL_TRUSTED_TABLE = "accelerometer_trusted"

TARGET_S3_PATH = "s3://stedi-dlbadley-2026/step_trainer/curated/"
TARGET_TABLE = "machine_learning_curated"

# -------------------------
# BOOTSTRAP GLUE
# -------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------
# SOURCES: Glue Catalog
# -------------------------
step_trainer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=STEP_TRAINER_TRUSTED_TABLE,
    transformation_ctx="step_trainer_trusted_dyf",
)

accelerometer_trusted_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=ACCEL_TRUSTED_TABLE,
    transformation_ctx="accelerometer_trusted_dyf",
)

CUSTOMER_CURATED_TABLE = "customer_curated"

customer_curated_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=CUSTOMER_CURATED_TABLE,
    transformation_ctx="customer_curated_dyf",
)

# -------------------------
# TRANSFORM (Spark SQL)
# Join per rubric: sensorReadingTime = timestamp
#
# Duplication control:
# - First, remove exact duplicates inside each input (DISTINCT on selected cols).
# - After join, compute a composite row key and keep 1 row per key using ROW_NUMBER().
#
# NOTE:
# - In Glue/Athena, your columns are typically lowercase versions of the JSON keys:
#   step_trainer: serialnumber, sensorreadingtime, distancefromobject
#   accelerometer: user, timestamp, x, y, z
# -------------------------
ml_sql = """
WITH
cc AS (
  SELECT lower(email) AS email_l, lower(serialnumber) AS serial_l
  FROM customer_curated
  WHERE email IS NOT NULL AND serialnumber IS NOT NULL
),
st AS (
  SELECT
    lower(serialnumber) AS serial_l,
    sensorreadingtime,
    distancefromobject
  FROM step_trainer_trusted
  WHERE serialnumber IS NOT NULL AND sensorreadingtime IS NOT NULL
),
acc AS (
  SELECT
    lower(user) AS email_l,
    timestamp,
    x, y, z
  FROM accelerometer_trusted
  WHERE user IS NOT NULL AND timestamp IS NOT NULL
)
SELECT
  acc.email_l              AS user,
  acc.timestamp            AS timestamp,
  acc.x, acc.y, acc.z,
  st.serial_l              AS serialnumber,
  st.sensorreadingtime     AS sensorreadingtime,
  st.distancefromobject    AS distancefromobject
FROM st
JOIN cc
  ON st.serial_l = cc.serial_l
JOIN acc
  ON acc.email_l = cc.email_l
 AND acc.timestamp = st.sensorreadingtime
"""

machine_learning_curated_dyf = spark_sql_query(
    glue_context=glueContext,
    query=ml_sql,
    mapping={
        "step_trainer_trusted": step_trainer_trusted_dyf,
        "accelerometer_trusted": accelerometer_trusted_dyf,
        "customer_curated": customer_curated_dyf,
    },
    transformation_ctx="machine_learning_curated_dyf",
)

# -------------------------
# TARGET: S3 + Auto Catalog Create/Update
# Write Parquet for performance + cheaper Athena scans
# -------------------------
glueContext.purge_s3_path(TARGET_S3_PATH, options={"retentionPeriod": 0})
sink = glueContext.getSink(
    path=TARGET_S3_PATH,
    connection_type="s3",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    transformation_ctx="machine_learning_curated_sink",
)

sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=TARGET_TABLE)
sink.setFormat("glueparquet")
sink.writeFrame(machine_learning_curated_dyf)

job.commit()
