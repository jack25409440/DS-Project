import sys, os
from contextlib import contextmanager
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import json
import re
import nltk
import numpy as np
from textblob import TextBlob
from math import log, fabs

NUM_OF_LINES = 41135700
#NUM_OF_LINES = 1000

OUTPUT_PROTOCOL = JSONValueProtocol

@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:  
            yield
        finally:
            sys.stdout = old_stdout

def feature_extractor_by_line(line):
    dic = {}
    data = json.loads(line)
    dic['reviewId'] = data['reviewId']
    dic['reviewerId'] = data['reviewerId']
    dic['helpfulRate'] = get_helpful_rate(data['helpful'][0], data['helpful'][1])
    dic['stars'] = data['stars']
    dic.update(get_sentiments(data['reviewText']))
    dic['reviewText'] = replace_text(data['reviewText'])
    return data['reviewId'], dic

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


class MRTfidfCalculator(MRJob):

    def __init__(self, *args, **kwargs):
        super(MRTfidfCalculator, self).__init__(*args, **kwargs)
	with suppress_stdout():
            nltk.download('stopwords')
        self.stop = nltk.corpus.stopwords.words('english')

    def word_frequency_mapper(self, _, line):
        docid, value = feature_extractor_by_line(line)
        text = value.pop('reviewText', '')
        words = nltk.tokenize.wordpunct_tokenize(text)
        for word in words:
            yield (word, docid), (1, value) 

    def word_frequency_combiner(self, term_docid, values):
        total = 0
        firstExecuted = False
        metaJson = {}
        for value in list(values):
            total+=value[0]
            if not firstExecuted:
                metaJson = value[1]
                firstExecuted = True
        yield term_docid, (total, metaJson)


    def word_frequency_reducer(self, term_docid, values):
        total = 0
        firstExecuted = False
        metaJson = {}
        for value in list(values):
            total+=value[0]
            if not firstExecuted:
                metaJson = value[1]
                firstExecuted = True
        yield term_docid, (total, metaJson)

    def word_count_mapper(self, term_docid, value):
        term, docid = term_docid
        total, metaJson = value
        yield docid, (term, total, metaJson)

    def word_count_reducer(self, docid, values):
        d = 0;
        term_total_metaJsons = list(values)
        firstExecuted = False
        metaJson = {}
        for term_total_metaJson in term_total_metaJsons:
            term = term_total_metaJson[0]
            total = term_total_metaJson[1]
            d += total
            if not firstExecuted:
                metaJson = term_total_metaJson[2]
                firstExecuted = True
        for term_total_metaJson in term_total_metaJsons:
            term = term_total_metaJson[0]
            total = term_total_metaJson[1]
            yield (term, docid), (total, d, metaJson)

    def corpus_frequency_mapper(self, term_docid, total_d_metaJson):
        term, docid = term_docid
        total, d, metaJson = total_d_metaJson
        yield term, (docid, total, d, 1, metaJson)

    def corpus_frequency_reducer(self, term, quadlets):
        quadlet_list = list(quadlets)
        n = len(quadlet_list)
        for quadlet in quadlet_list:
            docid = quadlet[0]
            total = quadlet[1]
            d = quadlet[2]
            metaJson = quadlet[4]
            yield (term, docid), (total, d, n, metaJson)

    def tfidf_mapper(self, term_docid, quadlet):
        term, docid = term_docid
        t, d, n, metaJson  = quadlet
        tf = (t*1.0)/(d*1.0)
        idf = log((NUM_OF_LINES*1.0)/(n*1.0))
        tfidf = tf*idf
        key = 'tfidf_'+term
        yield docid, ({key : tfidf}, metaJson)

    def tfidf_combiner(self, docid, tuples):
        tup_list = list(tuples)
        dic = {}
	firstExecuted = False
        metaJson = {}
        for tup in tup_list:
            dic.update(tup[0])
            if not firstExecuted:
                metaJson = tup[1]
                firstExecuted = True
        yield docid, (dic, metaJson)

    def tfidf_reducer(self, docid, tuples):
        dic = {}
	firstExecuted = False
        metaJson = {}
        for tup in list(tuples):
            dic.update(tup[0])
            if not firstExecuted:
                metaJson = tup[1]
                firstExecuted = True
        ret_dic = metaJson.copy()
        ret_dic.update(dic)
        yield None, ret_dic

    
    def steps(self):
        return [
            MRStep(mapper = self.word_frequency_mapper,
                   combiner = self.word_frequency_combiner,
                   reducer = self.word_frequency_reducer),
            MRStep(mapper = self.word_count_mapper,
                   reducer = self.word_count_reducer),
            MRStep(mapper = self.corpus_frequency_mapper,
                   reducer = self.corpus_frequency_reducer),
            MRStep(mapper = self.tfidf_mapper,
                   combiner = self.tfidf_combiner,
                   reducer = self.tfidf_reducer)
        ]

if __name__ == '__main__':
    MRTfidfCalculator.run()
