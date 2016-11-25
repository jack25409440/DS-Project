create table filtered_sentiments_v2
(
    reviewId                int     NOT NULL PRIMARY KEY,
    reviewerId              varchar NOT NULL SORTKEY,
    min_polarity            float   NOT NULL,
    helpfulRate             float   NOT NULL,
    avg_subjectivity        float   NOT NULL,
    max_subjectivity        float   NOT NULL,
    avg_polarity            float   NOT NULL,
    stars                   int     NOT NULL,
    magnitude_polarity      float   NOT NULL,
    max_polarity            float   NOT NULL,
    min_subjectivity        float   NOT NULL,
    max_min_or_max_polarity float   NOT NULL
);
