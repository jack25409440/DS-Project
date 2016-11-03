import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import json
import re
import nltk
from math import log, fabs

NUM_OF_LINES = 41135700
#NUM_OF_LINES = 1000

OUTPUT_PROTOCOL = JSONValueProtocol

def extract_text_by_line(line):
    data = json.loads(line)
    return data['reviewId'], replace_text(data['reviewText'])

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

    def word_frequency_mapper(self, _, line):
        docid, text = feature_extractor_by_line(line)
	stopwords = nltk.corpus.stopwords.words('english')
        words = nltk.tokenize.wordpunct_tokenize(text)
        for word in words:
            if word in stopwords:
                continue
            yield (word, docid), 1 

    def word_frequency_combiner(self, term_docid, counts):
        total = 0
        for count in counts:
            total+=count
        yield term_docid, total


    def word_frequency_reducer(self, term_docid, counts):
        total = 0
        for count in counts:
            total+=count
        yield term_docid, total


    def word_count_mapper(self, term_docid, total):
        term, docid = term_docid
        yield docid, (term, total)

    def word_count_reducer(self, docid, values):
        d = 0;
        term_totals = list(values)
        for term_total in term_totals:
            term = term_total[0]
            total = term_total[1]
            d += total
        for term_total in term_totals:
            term = term_totals[0]
            total = term_totals[1]
            yield (term, docid), (total, d)

    def corpus_frequency_mapper(self, term_docid, total_d):
        term, docid = term_docid
        total, d = total_d
        yield term, (docid, total, d)

    def corpus_frequency_reducer(self, term, triplets):
        triplet_list = list(triplets)
        n = len(triplet_list)
        for triplet in quadlet_list:
            docid = triplet[0]
            total = triplet[1]
            d = triplet[2]
            yield (term, docid), (total, d, n)

    def tfidf_mapper(self, term_docid, triplet):
        term, docid = term_docid
        t, d, n = triplet
        tf = (t*1.0)/(d*1.0)
        idf = log((NUM_OF_LINES*1.0)/(n*1.0 + 1.0))
        tfidf = tf*idf
        key = 'tfidf_'+term
        yield docid, {key : tfidf}

    def tfidf_combiner(self, docid, dics):
        return_dic = {}
        for dic in dics:
            return_dic.update(dic)
        yield docid, (dic, metaJson)

    def tfidf_reducer(self, docid, tuples):
        return_dic = {}
        for dic in dics:
            return_dic.update(dic)
        yield docid, (dic, metaJson)
    
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
