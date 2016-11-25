unload ('select * from filtered_sentiments_v2')
to 's3://amazon-review-data-emr/redshift-unload-v2.csv' 
credentials 
'aws_access_key_id=<access_key>;aws_secret_access_key=<secret_access_key>'
parallel off
ALLOWOVERWRITE
delimiter as ',';
