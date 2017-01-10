cp ../tax_return.py .

python map_xml_schemas.py -r emr s3://irs-form-990-hadoop/*.xml.gz --conf-path ~/.mrjob.conf --output-dir=s3://irs-form-990-hadoop/mr-output --region='us-east-1'
