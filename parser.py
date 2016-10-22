import sys
import pandas as pd
import json
import datetime
import time
import requests
from joblib import Parallel, delayed
import multiprocessing
from sklearn.feature_extraction.text import TfidfVectorizer
import re
from textblob import TextBlob
import numpy as np
import boto3

def read_as_df(file_path):
    with open(file_path, 'rU') as f:
        data = [json.loads(row) for row in f]
        return pd.DataFrame(data)

def get_helpful_rate(nominator, denominator):
    if denominator==0:
        return 0.0
    else:
        return (nominator*1.0)/(denominator*1.0)

def get_minmax(num_list):
    returnVal = 0.0
    for num in num_list:
        if abs(num) - returnVal > 10E-6 : 
            returnVal = num
    return returnVal

def get_sentiments(text):
    blob = TextBlob(text);
    polarities = [sentence.sentiment.polarity for sentence in blob.sentences]
    subjectivities = [sentence.sentiment.subjectivity for sentence in blob.sentences]
    if len(polarities) == 0:
        polarities = [0.0]
    if len(subjectivities) == 0:
        subjectivities = [0.0]
    minmax = get_minmax(polarities)
    return {'max_min_or_max_polarity':minmax, 'magnitude_polarity':abs(minmax), 'min_polarity':min(polarities), 'max_polarity':max(polarities), 'avg_polarity':np.mean(polarities),  'min_subjectivity':min(subjectivities), 'max_subjectivity':max(subjectivities), 'avg_subjectivity':np.mean(subjectivities)}

 
def extract_features_by_row(row):
    dic = {}
    dic['reviewerId'] = row['reviewerID']
    dic['helpfulRate'] = get_helpful_rate(row['helpful'][0], row['helpful'][1])
    dic['stars'] = row['overall']
    dic.update(get_sentiments(row['reviewText']))
    return dic

def extract_features(dataframe):
    num_cores = multiprocessing.cpu_count()
    result = Parallel(n_jobs=num_cores)(delayed(extract_features_by_row)(row) for index,row in dataframe.iterrows())
    return pd.DataFrame.from_records(result)

def save_as_csv(dataframe):
    ts = time.time()
    dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H:%M:%S')
    file_path = 'amazon_review_' + dt + '.csv'
    dataframe.to_csv(file_path)
    return file_path

def replace_text(text):
    text = re.sub(r'\b[0-9]+(st|nd|rd|th)\b', 'orderplaceholder', text)
    text = re.sub(r'\b1[0-9][0-9][0-9]\b', 'yearplaceholder', text)
    text = re.sub(r'\b[0-9]+s\b','decadeplaceholder', text)
    text = re.sub(r'\b[0-9]+(yr|yrs|year|years)\b', 'yearcountplaceholder', text)
    text = re.sub(r'\b[0-9]+(w|wk|wks|week|weeks)\b', 'weekcountplaceholder', text)
    text = re.sub(r'\b[0-9]+(m|month|months)\b', 'monthscountplaceholder', text)
    text = re.sub(r'\b[0-9]+(d|day|days)\b', 'dayscountplaceholder', text)
    text = re.sub(r'\b[0-9]+(t|ton|tons|lb|pound|pounds|kg|g|gram|grams|oz)\b', 'weightplaceholder', text)
    text = re.sub(r'\b[0-9][a-zA-z]+', 'othercountsplaceholder', text)
    text = re.sub(r'\b[0-9]+\b', 'numberplaceholder', text)
    return text

def tune_review_text(text_series):
    num_cores = multiprocessing.cpu_count()
    return Parallel(n_jobs=num_cores)(delayed(replace_text)(text) for text in text_series)

def extract_tfidf_features(text_series):
    vectorizer = TfidfVectorizer(min_df=1, use_idf=True, smooth_idf=True)
    X = vectorizer.fit_transform(tune_review_text(text_series))
    dataframe = pd.DataFrame(X.toarray(), columns=['tfidf_'+column for column in vectorizer.get_feature_names()])
    dataframe.index.rename('reviewId', inplace=True)
    return (vectorizer, dataframe)

def upload_to_s3(file_path):
    s3 = boto3.resource('s3')
    data = open(file_path, 'rb')
    s3.Bucket('amazon-review-data').put_object(Key=file_path, Body=data)

def main(argv):
    raw_dataframe = read_as_df(argv[0])
    raw_dataframe.index.rename('reviewId', inplace=True)
    transformed_dataframe = extract_features(raw_dataframe)
    tfidf_vectorizer, tfidf_dataframe = extract_tfidf_features(raw_dataframe['reviewText'])
    transformed_dataframe.index.rename('reviewId', inplace=True)
    transformed_dataframe = pd.concat([transformed_dataframe, tfidf_dataframe], axis=1)
    file_path = save_as_csv(transformed_dataframe)
    upload_to_s3(file_path)

if __name__ == "__main__":
    main(sys.argv[1:])
