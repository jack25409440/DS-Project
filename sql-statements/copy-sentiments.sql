copy sentiments
from 's3://amazon-review-data-emr/sentiments.json' 
credentials 'aws_access_key_id=<access_key>;aws_secret_access_key=<secret_access_key>'
format as json 's3://amazon-review-data-emr/sentiments-json-path.json';
