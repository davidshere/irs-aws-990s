import io

from boto.s3.connection import S3Connection
from lxml import etree

SOURCE_BUCKET = 'irs-form-990'
DEST_BUCKET = 'irs-form-990-hadoop'

source_conn = S3Connection()
#dest_conn = S3Connection()

source_bucket = source_conn.get_bucket(SOURCE_BUCKET)
#dest_bucket = dest_conn.get_bucket(DEST_BUCKET)

buff = io.StringIO()

def clean_xml_string(string):
	""" We want to get rid of newlines and excess whitespace """
	without_newlines = string.decode().replace('\r\n', '')
	return ' '.join([tag for tag in without_newlines.split('  ') if tag])

# download and clean the XML documents
return_set = etree.XML('<ReturnSet></ReturnSet>')
for i, item in enumerate(source_bucket.list()):
	if item.name.endswith('_public.xml'):
		raw_xml = item.read()
		clean_xml = clean_xml_string(raw_xml)
		single_return = etree.XML(clean_xml)
		return_set.append(single_return)
	
	if i == 99:
		break

with open('spark-test/minified.xml', 'wb') as f:
	f.write(etree.tostring(return_set))