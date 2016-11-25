import sys
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
import json
import re

def replace_text(text):
    text = text.lower()
    text = re.sub(r'\$[0-9]+', 'moneyplaceholder', text)
    text = re.sub(r'\$[0-9]+.[0-9]+', 'moneyplaceholder', text)
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
    text = re.sub(r'[^\w]', ' ', text)
    return text

def generate_corpus(csv_file_path):
    data = pd.read_csv(csv_file_path, index_col='reviewid')
    data.sort_index(inplace=True)
    corpus = []
    with open('transformed_kcore.json', 'rU') as raw_data_file:
        for row in raw_data_file:
            row_json = json.loads(row)
            if row_json['reviewId'] in data.index:
                corpus.append(replace_text(row_json['reviewText']))
                print "Added one to corpus: ", len(corpus)
    return data, corpus

def generate_tfidf_dataframe(origin_data, corpus):
    vectorizer = TfidfVectorizer(stop_words='english', max_features=100)
    X = vectorizer.fit_transform(corpus)
    feature_names = vectorizer.get_feature_names()
    transformed_feature_names = ['tfidf_'+feature for feature in feature_names]
    return pd.DataFrame(data=X.toarray(), index=origin_data.index, columns=transformed_feature_names)

def write_to_csv(data, tfidf_data):
   result = data.join(tfidf_data)
   result.to_csv('redshift-unload-with-tfidf.csv')

def main(argv):
    data, corpus = generate_corpus(argv[0])
    print 'Begin generating tfidf'
    tfidf_data = generate_tfidf_dataframe(data, corpus)
    print 'Finish generating tfidf'
    print 'Begin writing to csv'
    write_to_csv(data, tfidf_data)
    print 'Finish writing to csv'

if __name__=='__main__':
    main(sys.argv[1:])
