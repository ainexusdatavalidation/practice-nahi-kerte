import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1745596001451 = glueContext.create_dynamic_frame.from_catalog(database="company", table_name="s3_creweler_glue_catalog_redshift", transformation_ctx="AWSGlueDataCatalog_node1745596001451")

# Script generated for node Amazon Redshift
AmazonRedshift_node1745596009393 = glueContext.write_dynamic_frame.from_options(frame=AWSGlueDataCatalog_node1745596001451, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-780361502647-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.emp", "connectionName": "Redshift connection865", "preactions": "CREATE TABLE IF NOT EXISTS public.emp (age BIGINT, gender VARCHAR, review VARCHAR, education VARCHAR, purchased VARCHAR);"}, transformation_ctx="AmazonRedshift_node1745596009393")

job.commit()