import os
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from googleapiclient.discovery import build


def lambda_handler(event, context):
    # get the API key from Lambda environment variables
    youtube_api_key = os.environ['YOUTUBE_API_KEY']

    # define api variables
    api_name = 'youtube'
    api_version = 'v3'

    # define S3 variables
    bucket_name = '<s3-bucket-name>'

    # initialise the API client
    youtube = build(api_name, api_version, developerKey=youtube_api_key)

    # init s3 client
    s3 = boto3.client('s3')

    # set today's datetime for search
    search_date = (datetime.now(ZoneInfo('Australia/Melbourne')) - timedelta(1)).strftime('%Y-%m-%d')
    # get last 6 days for which data should be retrieved
    today = datetime.now(ZoneInfo('Australia/Melbourne'))
    search_date_list = [(today-timedelta(i)).strftime('%Y-%m-%d') for i in range(2,8)]

    # get the oldest date for which video data should be retrieved
    # convert string to datetime
    oldest_collection_date = datetime.strptime((today - timedelta(7)).strftime('%Y-%m-%d'), '%Y-%m-%d')

    # get data for last 7 days
    videos = []

    for day in search_date_list:
        directory_prefix = 'raw/videos'
        partition_prefix = f'{directory_prefix}/collection_date={day}'

        # check if partition exists in S3
        file_response = s3.list_objects_v2(Bucket=bucket_name, Prefix=partition_prefix)

        # break loop if latest partition is not available - means previous partitions aren't available as well
        if 'Contents' not in file_response:
            break
        else:
            # file path
            file_key = file_response.get('Contents')[0].get('Key')
            # file date in datetime format
            file_date = datetime.strptime(day, '%Y-%m-%d') 
            search_date_dt = datetime.strptime(search_date, '%Y-%m-%d')
            # get object
            file_obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            file_table = pq.read_table(BytesIO(file_obj['Body'].read()))
            video_ids = file_table.column('id').to_pylist()

            # get data for each video
            video_data_response = youtube.videos().list(
                part='statistics',
                id=','.join(video_ids)
            ).execute()

            for item in video_data_response.get('items', []):
                statistics = item.get('statistics', {})

                video = {
                    "id": item.get('id', ''),
                    "initialCollectionDate": file_date,
                    "collectionDate": search_date_dt,
                    "collectionCount": (search_date_dt-file_date).days + 1,
                    "viewCount": statistics.get('viewCount', '0'),
                    "likeCount": statistics.get('likeCount', '0'),
                    "favoriteCount": statistics.get('favoriteCount', '0'),
                    "commentCount": statistics.get('commentCount', '0')
                }
                videos.append(video)

    # save data in s3 if data is available
    if len(videos) > 0:
        # get column names
        columns = videos[0].keys()

        # convert list of dicts to list of lists
        videos_list = {key: [item[key] for item in videos] for key in columns}

        # convert list to pyarrow table
        videos_tb = pa.table(videos_list)

        # write to parquet file in memory
        parquet_buffer = BytesIO()
        pq.write_table(videos_tb, parquet_buffer)

        # upload to s3
        parquet_file_key = f'raw/video_stats/collection_date={search_date}/{search_date}.parquet'
        s3.put_object(Bucket=bucket_name, Key=parquet_file_key, Body=parquet_buffer.getvalue())
        print(f'Parquet file uploaded to s3://{bucket_name}/{parquet_file_key}')
    else:
        print(f'No data available before {search_date_list[0]}')
