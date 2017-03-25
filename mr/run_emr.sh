cp ../tax_return.py .
rm sources.zip
zip sources.zip tax_return.py

python $1 -r emr s3://irs-form-900-hadoop/$2 --conf-path ~/.mrjob.conf --output-dir=s3://irs-form-990-hadoop/$3 --region='us-east-1' 
#python $1 -r emr s3://irs-form-990-hadoop/$2 --conf-path ~/.mrjob.conf --output-dir=s3://irs-form-990-hadoop/$3 --region='us-east-1' --num-core-instances=$4 --instance-type=m3.xlarge
