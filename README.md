# Authorship Identification with Amazon Review Data

#### Author: Xiaohui Chen

## Objective

The purpose of this project is to identify the author given an anoynymous review text. Different people have different language 
styles and so applying techniques in Natural Language Processing, which could extract related features of review texts, could 
be used for predictions. 

## Steps

0. Preprocessing: Add reviewId(see `parser-json.py`)

1. Extract the sentiment features(see `parser.py`)

2. Use Amazon EMR to extract the tf-idf values of each token (see `mr_driver.py`)

3. Two files are uploaded to S3. One contains sentiments and the other contains tf-idf values. The files are in JSON format

4. Use `COPY` command to copy those two files to Amazon Redshift. One table for each file

5. Use SQL to join two tables into one according to review IDs

6. Filter the dataset. Keep only customers with 20 or more reviews

7. Test-train split

8. Create model using Amazon Machine Learning

9. Batch predict with test data. Create data visualizations

## Sentiment Features

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

The Tf-idf Features starts with 'tfidf-'. If a token does not exist in this review text, the tfidf value is 0. The tfidf calcuation is implemented by 4 sets of mappers and reducers. 

## Model Evaluation

TODO

## Conclusion

TODO
