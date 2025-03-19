import sys
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, concat_ws, to_timestamp, to_date, current_timestamp, length, substring
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from awsglue.job import Job

# initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# parameters
bucket_uri = 's3://<s3-bucket-name>'

database_name = '<your-database>'
redshift_video_table_name = 'videos'
redshift_stats_table_name = 'video_stats'
redshift_temp_dir = f'{bucket_uri}/temp/'
redshift_connection = 'Jdbc-redshift-connection'
redshift_jdbc_url = 'jdbc:redshift://your-cluster-endpoint:5439/your-database'

# process the latest file stored in S3
search_date = (datetime.now(ZoneInfo('Australia/Melbourne'))-timedelta(1)).strftime('%Y-%m-%d')
videos_file_path = f'{bucket_uri}/raw/videos/collection_date={search_date}/{search_date}.parquet'
stats_file_path = f'{bucket_uri}/raw/video_stats/collection_date={search_date}/{search_date}.parquet'

# load datasets from S3
videos_datasource = glueContext.create_dynamic_frame.from_options(
    connection_type='s3',
    connection_options={
        'paths': [videos_file_path]
    },
    format='parquet'
)
videostats_datasource = glueContext.create_dynamic_frame.from_options(
    connection_type='s3',
    connection_options={
        'paths': [stats_file_path]
    },
    format='parquet'
)

# convert DynamicFrames to DataFrames
videos_df = videos_datasource.toDF()
stats_df = videostats_datasource.toDF()

# rename columns in videos data
videos_df_renamed = videos_df \
                        .withColumnRenamed('id', 'video_id') \
                        .withColumnRenamed('title', 'video_title') \
                        .withColumnRenamed('description', 'video_description') \
                        .withColumnRenamed('publishedAt', 'video_published_datetime') \
                        .withColumnRenamed('channelId', 'channel_id') \
                        .withColumnRenamed('channelTitle', 'channel_title') \
                        .withColumnRenamed('videoCategoryId', 'video_category_id') \
                        .withColumnRenamed('videoDuration', 'video_duration') \
                        .withColumnRenamed('videoDefinition', 'video_definition') \
                        .withColumnRenamed('collectionDate', 'collection_date')

# convert video_tags field to a string
videos_df_renamed = videos_df_renamed.withColumn('video_tags_truncated', concat_ws(',','tags'))

# add columns
videos_df_modified = videos_df_renamed \
                        .withColumn('ingested_datetime', current_timestamp()) \
                        .withColumn('video_description_length', length(col('video_description'))) \
                        .withColumn('video_description_truncated', substring(col('video_description'), 1, 100)) \
                        .withColumn('video_tags_truncated', substring(col('video_tags_truncated'), 1, 300))

# cast data types
videos_df_modified = videos_df_modified \
                        .withColumn('video_published_datetime', to_timestamp(col('video_published_datetime'),"yyyy-MM-dd'T'HH:mm:ss'Z'")) \
                        .withColumn('collection_date', to_date(col('collection_date'), "yyyy-MM-dd")) \
                        .withColumn('video_category_id', col('video_category_id').cast('int')) \
                        .withColumn('ingested_datetime', to_timestamp(col('ingested_datetime'), "yyyy-MM-dd HH:mm:ss.SSS"))

# reorder columns
videos_cols = ['video_id', 'video_title', 'video_description_truncated', 'video_description_length', 
               'video_published_datetime', 'channel_id', 'channel_title', 'video_category_id', 
               'video_tags_truncated', 'video_duration', 'video_definition', 'collection_date', 'ingested_datetime']
videos_data = videos_df_modified.select([col(c) for c in videos_cols])

# ------------
# combine initial stats with video stats data

# format columns from video data to match stats data format
initial_stats_df = videos_df.select(
                        col('id').alias('video_id'),
                        to_date(col('collectionDate'), "yyyy-MM-dd").alias('initial_collection_date'),
                        to_date(col('collectionDate'), "yyyy-MM-dd").alias('collection_date'),
                        lit(1).alias('collection_count'),
                        col('initialViewCount').alias('view_count'),
                        col('initialLikeCount').alias('like_count'),
                        col('initialFavoriteCount').alias('favorite_count'),
                        col('initialCommentCount').alias('comment_count')
)

# rename columns in stats data
stats_df_renamed = stats_df \
                .withColumnRenamed('id', 'video_id') \
                .withColumnRenamed('initialcollectiondate', 'initial_collection_date') \
                .withColumnRenamed('collectiondate', 'collection_date') \
                .withColumnRenamed('collectioncount', 'collection_count') \
                .withColumnRenamed('viewcount', 'view_count') \
                .withColumnRenamed('likecount', 'like_count') \
                .withColumnRenamed('favoritecount', 'favorite_count') \
                .withColumnRenamed('commentcount', 'comment_count') 

# union initial stats and stats data
stats_df_modified = initial_stats_df
if not stats_df_renamed.isEmpty():
    # cast data types
    stats_df_renamed = stats_df_renamed \
                        .withColumn('initial_collection_date', to_date(col('initial_collection_date'), "yyyy-MM-dd HH:mm:ss")) \
                        .withColumn('collection_date', to_date(col('collection_date'), "yyyy-MM-dd HH:mm:ss"))

    stats_df_modified = stats_df_renamed.union(initial_stats_df)

# add columns
stats_df_modified = stats_df_modified.withColumn('ingested_datetime', current_timestamp())

# cast data types
stats_data = stats_df_modified.select(
                        col('video_id'),
                        col('initial_collection_date'),
                        col('collection_date'),
                        col('collection_count').cast('int').alias('collection_count'),
                        col('view_count').cast('int').alias('view_count'),
                        col('like_count').cast('int').alias('like_count'),
                        col('favorite_count').cast('int').alias('favorite_count'),
                        col('comment_count').cast('int').alias('comment_count'),
                        to_timestamp(col('ingested_datetime'), "yyyy-MM-dd HH:mm:ss.SSS").alias('ingested_datetime')
)

videos_data.printSchema()
stats_data.printSchema()

# convert back to DynamicFrame
videos_dynamic_frame = DynamicFrame.fromDF(videos_data, glueContext, 'videos_dynamic_frame')
stats_dynamic_frame = DynamicFrame.fromDF(stats_data, glueContext, 'stats_dynamic_frame')

# write to Redshift
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=videos_dynamic_frame,
    catalog_connection=redshift_connection,
    connection_options={
        'dbtable': redshift_video_table_name,
        'database': database_name
    },
    redshift_tmp_dir=redshift_temp_dir
)

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=stats_dynamic_frame,
    catalog_connection=redshift_connection,
    connection_options={
        'dbtable': redshift_stats_table_name,
        'database': database_name
    },
    redshift_tmp_dir=redshift_temp_dir
)

job.commit()
