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
    parquet_file_key = f'raw/videos/collection_date={search_date}/{search_date}.parquet'

    # initialise the API client
    youtube = build(api_name, api_version, developerKey=youtube_api_key)

    # set datetime for search
    search_date = (datetime.now(ZoneInfo('Australia/Melbourne')) - timedelta(1)).strftime('%Y-%m-%d')
    previous_date = (datetime.now(ZoneInfo('Australia/Melbourne')) - timedelta(2)).strftime('%Y-%m-%d')
    utc_time = 'T13:30:00Z'
    search_date_utc = search_date + utc_time
    previous_date_utc = previous_date + utc_time

    # execute search request for 50 videos with the highest view count
    search_response = youtube.search().list(
        part='id',
        q='japan + travel',
        type='video',
        maxResults=50,
        publishedAfter=previous_date_utc,
        publishedBefore=search_date_utc,
        order='viewCount',
        topicId='/m/07bxq',
        videoDuration='medium'
    ).execute()

    # get video ids
    video_ids = [item['id']['videoId'] for item in search_response.get('items', [])]

    # get data for each video
    video_data_response = youtube.videos().list(
        part='snippet,content_details,statistics',
        id=','.join(video_ids)
    ).execute()

    # extract video data
    videos = []
    for item in video_data_response.get('items', []):
        # get() function allows a fallback value in case the element is not found in the video_data_response
        snippet = item.get('snippet', {})
        content_details = item.get('contentDetails', {})
        statistics = item.get('statistics', {})

        video = {
            "id": item.get('id', ''),
            "title": snippet.get('title', ''),
            "description": snippet.get('description', ''),
            "publishedAt": snippet.get('publishedAt'),
            "channelId": snippet.get('channelId', ''),
            "channelTitle": snippet.get('channelTitle', ''),
            "videoCategoryId": snippet.get('categoryId', 0),
            "tags": snippet.get('tags', []),
            "videoDuration": content_details.get('duration', ''),
            "videoDefinition": content_details.get('definition', ''),
            "initialViewCount": statistics.get('viewCount', '0'),
            "initialLikeCount": statistics.get('likeCount', '0'),
            "initialFavoriteCount": statistics.get('favoriteCount', '0'),
            "initialCommentCount": statistics.get('commentCount', '0'),
            "collectionDate": search_date
        }
        videos.append(video)

    # get column names
    columns = videos[0].keys()

    # convert list of dicts to list of lists
    videos_list = {key: [item[key] for item in videos] for key in columns}

    # convert list to pyarrow table
    videos_tb = pa.table(videos_list)

    # init s3 client
    s3 = boto3.client('s3')

    # write to parquet file in memory
    parquet_buffer = BytesIO()
    pq.write_table(videos_tb, parquet_buffer)

    # upload to s3
    s3.put_object(Bucket=bucket_name, Key=parquet_file_key, Body=parquet_buffer.getvalue())
    print(f'Parquet file uploaded to s3://{bucket_name}/{parquet_file_key}')
