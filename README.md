# Authorship Identification with Amazon Review Data

#### Author: Xiaohui Chen

## Objective

The purpose of this project is to identify the author given an anoynymous review text. Different people have different language 
styles and so applying techniques in Natural Language Processing, which could extract related features of review texts, could 
be used for predictions. 

## Dataset

The dataset is called [**Amazon product data**](http://jmcauley.ucsd.edu/data/amazon/) created by [Julian McAuley](http://cseweb.ucsd.edu/~jmcauley/). 
I used the 5-core data, in which all the reviewers in this dataset should have at least 5 reviews. The uncompressed json file 
occupies 30GB of disk space and there are 41,135,700 rows. one of the rows is shown as follows:

```json
{
  "reviewerID": "A2SUAM1J3GNN3B",
  "asin": "0000013714",
  "reviewerName": "J. McDonald",
  "helpful": [2, 3],
  "reviewText": "I bought this for my husband who plays the piano.  He is having a wonderful time playing these old hymns.  The music  is at times hard to read because we think the book was published for singing from more than playing from.  Great purchase though!",
  "overall": 5.0,
  "summary": "Heavenly Highway Hymns",
  "unixReviewTime": 1252800000,
  "reviewTime": "09 13, 2009"
}
```

The items are in JSON format. In fact, the keys need in this project are: `reviewerID, helpful, overall, reviewText`

## Steps

Initially, I was using Amaozn EMR to calculate the Tdidf values using MapReduce (see [here](https://github.com/louismullie/tf-idf-emr)). 
This is because 41,135,700 documents are too large to fit in the memory of a single computer. However, because of the limit of Amazon Machine Learning, 
which only allows 100 targets for multi-class classification, we are filtering the data and choose the reviews 
whose reviewers are having top 100 most reviews. With the help of redshift, we can get the filtered reviewId quickly. The filtered dataset only has 237,750 rows. This means I could use TfidfVectorizer in scikit-learn to extract the tfidf values. 

#### Processing Raw Data

* Preprocessing: Add reviewId(see `raw-data-parser-json.py`), calculate helpfulRate and change `overall` to `stars`

```
python raw-data-parser-json.py kcore_5.json
```

The above command generates a file named `transformed_kcore.json`

`kcore_5.json` could be downloaded [here](https://s3-us-west-2.amazonaws.com/amazon-review-data-emr/kcore_5.json)

#### Extracting Sentiment Features

* Extract the sentiment features using [TextBlob](https://textblob.readthedocs.io/en/dev/) (see `sentiment-parser.py`)

```
python sentiment-parser.py transformed_kcore.json
```

The above command generates a file called `sentiments.json`

`transformed_kcore.json` could be downloaded [here](https://s3-us-west-2.amazonaws.com/amazon-review-data-emr/transformed_kcore.json)

`sentiments.json` could be downloaded [here](https://s3-us-west-2.amazonaws.com/amazon-review-data-emr/sentiments.json)

#### Handling Sentiment Data in Redshift

1. Setup a redshift cluster. Make sure you set up IAM accounts and permissions properly

2. Create a table named `sentiments` (see `sql-statements/create-sentiments-table.sql`)

```sql
create table sentiments 
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
```

4. Use `COPY` command to copy `sentiments.json` to Amazon Redshift (see `sql-statements/copy-sentiments.sql`)

```sql
copy sentiments
from 's3://amazon-review-data-emr/sentiments.json' 
credentials 'aws_access_key_id=<access_key>;aws_secret_access_key=<secret_access_key>'
format as json 's3://amazon-review-data-emr/sentiments-json-path.json';
```

4. Create a table named `filtered_sentiments_v2`, which has the same attributes as `sentiments` table (see `sql-statements/filtered-sentiments-table.sql`)

```sql
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
```

6. Filter the data in `sentiments` and insert the filtered data to `filtered_sentiments_v2` (see `sql-statements/filter-sentiments.sql`)

```sql
insert into filtered_sentiments_v2
select * from sentiments
where sentiments.reviewerId in 
(select top 100 reviewerId from (select reviewerId, count(*) 
as NUM_OF_REVIEWS from sentiments group by reviewerId order by NUM_OF_REVIEWS desc) 
where NUM_OF_REVIEWS >= 200 and NUM_OF_REVIEWS <= 10000);
```

7. Unload the filtered dataset to a csv file and add column names in the fisrt row (see `sql-statements/unload-sentiments.sql`)

```sql
unload ('select * from filtered_sentiments_v2')
to 's3://amazon-review-data-emr/redshift-unload-v2.csv' 
credentials 
'aws_access_key_id=<access_key>;aws_secret_access_key=<secret_access_key>'
parallel off
ALLOWOVERWRITE
delimiter as ',';
```

The unloaded csv file could be found at `csv-data/redshift-unload-v2.csv`

#### Extracting Tf-idf features

* Extract tf-idf features using `filter-and-generate-corpus.py`

#### Creating Machine Learning Models using Amazon Machine Learning

8. Create datasets, models and evaluations using Amazon Machine Learning

9. Examine the dataset and evaluations. Create data visualization 

## Features other than Tf-idf

In each review text, each sentence has a polarity value and a subjectivity value. Polarity is ranging from -1.0 to 1.0. Here -1.0 means the sentence is negative and 1.0 means the sentence is positive. 0 means the sentence is neutral.

Meanwhile subjectivity is ranging from 0.0 to 1.0. Here 0.0 means the sentence is not subjective and 1.0 means the sentence is the most subjective

* `helpfulRate: #-helpful-vote/#-total-vote. If the total number of vote is 0, the helful rate is 0`

* `stars: the number of stars given in this review`

* `max_min_or_max_polarity: the polarity value whose absolute value is the highest`

* `magnitude_polarity: the highest absolute polarity value`

* `min_polarity: the minimun polarity values`

* `max_polarity: the maximum polarity values`

* `avg_polarity: the average polarity value`

* `min_subjectivity: the minimum subjectivity values`

* `max_subjectivity: the maxinum subjectivity values`

* `avg_subjectivity: the average subjectivity value`

## Tf-idf Features

The Tf-idf Features starts with 'tfidf\_'. If a token does not exist in this review text, the tfidf value is 0. ~~The tf-idf calcuation is implemented by 4 sets of mappers and reducers.~~ The tf-idf features are extracted by using TfidfVectorizer in scikit-learn. Note that we are extracting 100 tf-idf features whose tokens has top 100 tf-idf values. 

## Machine Learning Models

Two models are created. One without tf-idf features and the other with 100 tf-idf features for each review. Each model has its own datasource and the datasource are ramdomly splitted into 70% trainning and 30% testing. The testing data are used in the evaluations.

The models are evaluated by F1 scores. F1 scores are used to evaluate the quality of Machine Learning Models.

F1 score is a binary classification metric that considers both binary metrics precision and recall. It is the harmonic mean between precision and recall. The range is 0 to 1. A larger value indicates better predictive accuracy:

![](http://docs.aws.amazon.com/machine-learning/latest/dg/images/image53.png)

The macro average F1 score is the unweighted average of the F1-score over all the classes in the multiclass case. It does not take into account the frequency of occurrence of the classes in the evaluation dataset. A larger value indicates better predictive accuracy. The following example shows K classes in the evaluation datasource:

![](http://docs.aws.amazon.com/machine-learning/latest/dg/images/image54.png)

### Model without tf-idf features

#### Training Datasource

TODO

#### Testing Datasource

TODO

#### Evluation

TODO

### Model with tf-idf features

TODO

#### Training Datasource

TODO

#### Testing Datasource

TODO

#### Evluation

TODO

## Conclusion

TODO
