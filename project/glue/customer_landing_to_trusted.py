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
SRC_TABLE = "customer_landing"

TARGET_S3_PATH = "s3://stedi-dlbadley-2026/customer/trusted/"
TARGET_TABLE = "customer_trusted"

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

customer_landing_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=SRC_TABLE,
    transformation_ctx="customer_landing_dyf"
)

consented_sql = """
SELECT *
FROM customer_landing
WHERE shareWithResearchAsOfDate IS NOT NULL
  AND CAST(shareWithResearchAsOfDate AS string) <> ''
"""

customer_trusted_dyf = spark_sql_query(
    glue_context=glueContext,
    query=consented_sql,
    mapping={"customer_landing": customer_landing_dyf},
    transformation_ctx="customer_trusted_dyf"
)

glueContext.purge_s3_path(TARGET_S3_PATH, options={"retentionPeriod": 0})


sink = glueContext.getSink(
    path=TARGET_S3_PATH,
    connection_type="s3",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    transformation_ctx="customer_trusted_sink"
)

sink.setCatalogInfo(catalogDatabase=DATABASE, catalogTableName=TARGET_TABLE)
sink.setFormat("json")
sink.writeFrame(customer_trusted_dyf)

job.commit()
