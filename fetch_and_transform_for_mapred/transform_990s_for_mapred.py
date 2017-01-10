from collections import defaultdict
import io
import gzip

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from lxml import etree

from tax_return import TaxReturn

SOURCE_BUCKET = 'irs-form-990'
DEST_BUCKET = 'irs-form-990-hadoop'

conn = S3Connection()

source_bucket = conn.get_bucket(SOURCE_BUCKET)
dest_bucket = conn.get_bucket(DEST_BUCKET)

# initialize a variable to store documents. The keys will be versions, we'll
# store them in their own files.
DOCUMENTS = defaultdict(list)
FILES = {}

def clean_xml_string(string):
	""" We want to get rid of newlines and excess whitespace """
	without_newlines = string.replace(b'\r\n', b'')
	split_tags = [tag for tag in without_newlines.split(b'  ') if tag]
	return b' '.join(split_tags)

def process_single_return(item):
	""" Turn a single Key into an lxml._Element object,
		removing unneccesary characters and adding information
		about the return itself (i.e. object_id)
	"""
	raw_xml = item.read()
	clean_xml = clean_xml_string(raw_xml)
	single_return = etree.XML(clean_xml)
	version = single_return.get('returnVersion')
	object_id = item.name.split('_')[0]
	single_return.append(etree.XML('<ObjectID>%s</ObjectID>' % object_id))
	single_return = etree.tostring(single_return)
	return single_return, version

def join_and_compress_returns(version):
	pass

def fetch_processed_returns(tracker=None, existing_filenames=[]):
	""" Fetches XML files from S3, does some transformation,
		combines them until they reach a certain size, and then
		yields the lxml._Element.
	"""
	fp = io.BytesIO()

	for i, item in enumerate(source_bucket.list()):
		fname = item.name
		if fname.endswith('_public.xml') and fname not in existing_filenames:
			single_doc, version = process_single_return(item)
			DOCUMENTS[version].append(single_doc)

			if i % 100 == 0: 
				print(i)
				print([{doc_ver: len(DOCUMENTS[doc_ver])} for doc_ver in DOCUMENTS])

			if len(DOCUMENTS[version]) == 25000:

				docs = b'\n'.join(DOCUMENTS[version])
				compressed = gzip.compress(docs)
				DOCUMENTS[version] = []
				yield compressed, version

def upload_files_to_s3(bucket, return_string, version, filename):
	key = Key(bucket=bucket, name=filename)
	key.set_contents_from_string(return_string)

def write_files_to_disk(version, return_string, filename):
	with open(filename , 'wb') as f: 
		f.write(return_string)

if __name__ == "__main__":
	for j, data in enumerate(fetch_processed_returns()):
		returns, version = data
		filename = 'v%s_id%d.xml.gz' % (version, id(returns))
		print("Writing %s" % filename)
		upload_files_to_s3(dest_bucket, returns, version, filename)
		#write_files_to_disk(version, returns, filename)
		if j == 2:
			raise Exception

	for version in DOCUMENTS:
		returns = DOCUMENTS[version]
		filename = 'v%s_id%d.xml.gz' % (version, id(returns))
		upload_files_to_s3(dest_bucket, returns, filename)