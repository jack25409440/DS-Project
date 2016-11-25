import sys
import json
from sets import Set

def get_all_qualified_features():
    all_features = Set()
    file_prefix = 'tfidf/part-00'
    for file_index in range(158):
        file_name=file_prefix+'{foo:03d}'.format(foo=file_index)
        with open(file_name, 'rU') as infile:
            for row in infile:
                docid_features = row.split('\t')
                data=json.loads(docid_features[1])
                for key,value in data.iteritems():
                    if value-15.0 > 10E-6 and '__' not in key:
                        all_features.add(key)
        print 'Finish scanning file: '+file_name
        print 'Number of qualified features: '+str(len(all_features))
    return all_features

def clean_up_features(all_features):
    with open('cleaned_tfidf.json', 'w+') as outfile:
        file_prefix = 'tfidf/part-00'
        for file_index in range(158):
            file_name=file_prefix+'{foo:03d}'.format(foo=file_index)
            with open(file_name, 'rU') as infile:
                for row in infile:
                    docid_features = row.split('\t')
                    data=json.loads(docid_features[1])
                    data_copy = {}
                    for key,value in data.iteritems():
                        if key in all_features:
                            data_copy[key]=value
                    for feature in all_features:
                        if feature not in data_copy:
                            data_copy[feature]=0.0
                    data_copy['reviewId']=docid_features[0]
                    json.dump(data_copy, outfile)
                    outfile.write('\n')
            print 'Finish cleaning file: '+file_name


def generate_redshift_ddl_and_json_path(all_features):
    json_path_list=[]
    with open('sql-statements/create-tfidf-table.sql', 'w+') as outfile:
        print 'Begin generating DDL statement'
        outfile.write('create table tfidf\n')
        outfile.write('(\n')
        for feature in all_features:
            outfile.write(feature+' float NOT NULL,\n')
            json_path_list.append("$['"+ feature +"']")
        outfile.write('reviewId int NOT NULL PRIMARY KEY\n')
        json_path_list.appned("$['reviewId']")
        outfile.write(');\n')
        print 'Finish generating DDL statement'
    json_path_dic = {"jsonpaths":json_path_list}
    with open('tfidf-json-path.json', 'w+') as outfile:
        print 'Begin creating json path file'
        json.dump(json_path_dic, outfile)
        print 'Finish creating json path file'

def main():
    all_features = get_all_qualified_features()
    clean_up_features(all_features)
    generate_redshift_ddl_and_json_path(all_features)

if __name__=="__main__":
    main()
