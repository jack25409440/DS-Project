import sys
import json

def transfer_json(file_name):
    with open('transformed_kcore.json', 'w+') as outfile:
        with open(file_name, 'rU') as infile:
            reviewId=0
            for row in infile:
                data=json.loads(row)
                transformed_data={'reviewId':reviewId, 'reviewerId':data['reviewerID'], 'helpful':data['helpful'], 'reviewText':data['reviewText'], 'stars':data['overall']}
                json.dump(transformed_data, outfile)
                outfile.write('\n')
                if reviewId % 100000 == 0:
                    print 'proceseed ', reviewId, ' records'
                reviewId+=1
            

def main(argv):
    transfer_json(argv[0])

if __name__=="__main__":
    main(sys.argv[1:])
