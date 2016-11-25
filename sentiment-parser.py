import sys
import json
import time
import requests
import re
from textblob import TextBlob
import numpy as np
import boto3

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
    polarities = []
    subjectivities = []

    try:
        polarities = [sentence.sentiment.polarity for sentence in blob.sentences]
    except AttributeError:
        polarities = [0.0]
	sys.stderr.write("AttributeError in getting polarities: {0}".format(text))
    try:
        subjectivities = [sentence.sentiment.subjectivity for sentence in blob.sentences]
    except AttributeError:
        subjectivities = [0.0]
	sys.stderr.write("AttributeError in getting subjectivities: {0}".format(text))
    
    if len(polarities) == 0:
        polarities = [0.0]
    if len(subjectivities) == 0:
        subjectivities = [0.0]
    minmax = get_minmax(polarities)
    return {'max_min_or_max_polarity':minmax, 'magnitude_polarity':abs(minmax), 'min_polarity':min(polarities), 'max_polarity':max(polarities), 'avg_polarity':np.mean(polarities),  'min_subjectivity':min(subjectivities), 'max_subjectivity':max(subjectivities), 'avg_subjectivity':np.mean(subjectivities)}


def feature_extractor_by_line(line):
    dic = {}
    data = json.loads(line)
    reviewId = data['reviewId']
    dic['reviewId'] = reviewId
    dic['reviewerId'] = data['reviewerId']
    dic['helpfulRate'] = get_helpful_rate(data['helpful'][0], data['helpful'][1])
    dic['stars'] = data['stars']
    dic.update(get_sentiments(data['reviewText']))
    if reviewId % 100000 == 0:
        print 'proceseed ', reviewId, ' records'
    return dic
 
def upload_to_s3(file_path):
    s3 = boto3.resource('s3')
    data = open(file_path, 'rb')
    s3.Bucket('amazon-review-data-emr').put_object(Key=file_path, Body=data)

def main(argv):
    with open(argv[1], 'w+') as outfile:
        with open(argv[0], 'rU') as infile:
            for row in infile:
                dic = feature_extractor_by_line(row)
                json.dump(dic, outfile)
                outfile.write('\n')
    print('start uploading to s3')
    upload_to_s3(argv[1])
    print('finish uploading to s3')

if __name__ == "__main__":
    main(sys.argv[1:])
