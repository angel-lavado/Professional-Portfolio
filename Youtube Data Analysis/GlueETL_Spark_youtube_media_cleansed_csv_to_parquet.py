import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Retrieve job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Create SparkContext and GlueContext
sparkContext = SparkContext()
glueContext = GlueContext(sparkContext)
spark = glueContext.spark_session

# Create Glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Regions to be considered for the analysis
predicate_pushdown = "region in ('ca','gb','us')"

# Getting data from Data Source in S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="youtube-media-raw",
    table_name="raw_statistics",
    transformation_ctx="S3bucket_node1",
    push_down_predicate = predicate_pushdown
)

# Applying changes to the schema
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "bigint"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "bigint"),
        ("likes", "long", "likes", "bigint"),
        ("dislikes", "long", "dislikes", "bigint"),
        ("comment_count", "long", "comment_count", "bigint"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

### df_datasink1 & df_final_output steps are needed for creating partitions in S3bucket_node3 step

# Convert ApplyMapping_node2 to a DataFrame and coalesce/join the partitions into 1 for efficient writing
df_datasink1 = ApplyMapping_node2.toDF().coalesce(1)

# Convert the DataFrame datasink1 to a DynamicFrame for compatibility with Glue features
df_final_output = DynamicFrame.fromDF(df_datasink1, glueContext, "df_final_output")

# Writing data to S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=df_final_output,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://youtube-media-cleansed-useast1-dev/youtube/raw_statistics/",
        "partitionKeys": ["region"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3"
)

job.commit()

