CREATE TABLE videos (
    video_id                        VARCHAR(255)
    ,video_title                    VARCHAR(500)
    ,video_description_truncated    VARCHAR(500)
    ,video_description_length       INT
    ,video_published_datetime       TIMESTAMP
    ,channel_id                     VARCHAR(255)
    ,channel_title                  VARCHAR(500)
    ,video_category_id              INT
    ,video_tags_truncated           VARCHAR(500)
    ,video_duration                 VARCHAR(20)
    ,video_definition               VARCHAR(20)
    ,collection_date                DATE
    ,ingested_datetime              TIMESTAMP
);

CREATE TABLE video_stats (
    video_id                    VARCHAR(255)
    ,initial_collection_date    DATE
    ,collection_date            DATE
    ,collection_count           INT
    ,view_count                 INT
    ,like_count                 INT
    ,favorite_count             INT
    ,comment_count              INT
    ,ingested_datetime          TIMESTAMP
);