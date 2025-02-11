import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1739267570775 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-bucket-lakehouse/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1739267570775")

# Script generated for node Amazon S3
AmazonS3_node1739267618255 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-bucket-lakehouse/step-trainer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1739267618255")

# Script generated for node Join
Join_node1739267655165 = Join.apply(frame1=AmazonS3_node1739267618255, frame2=AmazonS3_node1739267570775, keys1=["sensorReadingTime"], keys2=["timestamp"], transformation_ctx="Join_node1739267655165")

# Script generated for node Drop Fields
DropFields_node1739267779606 = DropFields.apply(frame=Join_node1739267655165, paths=["birthDay", "shareWithPublicAsOfDate", "shareWithResearchAsOfDate", "registrationDate", "customerName", "shareWithFriendsAsOfDate", "`.serialNumber`", "lastUpdateDate", "`.birthDay`", "`.shareWithPublicAsOfDate`", "`.shareWithResearchAsOfDate`", "`.registrationDate`", "`.customerName`", "`.shareWithFriendsAsOfDate`", "`.lastUpdateDate`"], transformation_ctx="DropFields_node1739267779606")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1739267779606, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739267566326", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1739267851140 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1739267779606, connection_type="s3", format="json", connection_options={"path": "s3://stedi-bucket-lakehouse/ml-curated/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1739267851140")

job.commit()