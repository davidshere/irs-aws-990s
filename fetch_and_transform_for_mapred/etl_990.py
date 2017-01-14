from collections import defaultdict
import gzip
import io
import socket
import os

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from lxml import etree
import psycopg2
import requests


SOURCE_BUCKET = 'irs-form-990'
DEST_BUCKET = 'irs-form-990-hadoop'
S3_FOLDER = 'line-delimited-xml/'

DOCS_PER_FILE = 30000

# fetch everything from s3 and write to disk
# fetch everything from s3 and write to s3
# fetch from a sql query and write to disk
# fetch from a sql query and write to s3

class ReturnETL(object):
	""" A class to fetch raw 990s, transform them into a particular shape,
		and write them to some output source. 
	"""
	def __init__(self, 
				 source='boto', 
				 destination='s3', 
				 style='line-delimited',
				 dest_path = '',
				 source_bucket=None,
				 dest_bucket=None,
				 query=None):
		# these attributes are used regardless of source or destination
		self.documents = defaultdict(list)
		self.source = source
		self.destination = destination
		self.style = style
		self.dest_path

		# if we're getting returns via url instead of boto, we'll need
		# a query to run
		self.query = query

		# if we're getting returns with boto, we'll 
		# need a source bucket
		self.source_bucket = source_bucket

		# if we're writing returns to S3, we'll need a bucket
		self.dest_bucket = dest_bucket

	## methods to process an individual xml document
	def _clean_xml_string(self, string):
		""" We want to get rid of newlines and excess whitespace """
		without_newlines = string.replace(b'\r\n', b'')
		split_tags = [tag for tag in without_newlines.split(b'  ') if tag]
		return b' '.join(split_tags)

	def process_single_return(self, raw_xml):
		""" Turn a single Key into an lxml._Element object,
			removing unneccesary characters and adding information
			about the return itself (i.e. object_id)
		"""
		clean_xml = _clean_xml_string(raw_xml)
		single_return = etree.XML(clean_xml)

		version = single_return.get('returnVersion')
		object_id = item.name.split('_')[0]
		
		single_return.append(etree.XML('<ObjectID>%s</ObjectID>' % object_id))
		single_return = etree.tostring(single_return)
		return single_return, version

	## iterators to that return raw xml for processing
	def sql_iterator(self):
		conn = psycopg2.connect(os.environ['DB_URI'])
		cur = conn.cursor()
		cur.execute(self.query)
		for result in cur.iterate():
			r = requests.get(url)
			yield r.content

	def s3_iterator(self):
		for key in bucket.list():
			fname = key.name
			if fname.endswith('_public.xml'):	
				# recover in case of a timeout
				# but how the hell do you test this?
				while True:
					try:
						yield key.read()
						break
					except socket.timeout:
						continue
			else:
				continue

	def process_documents_for_writing(self, version):
		if self.style=='line-delimited':
			docs = b'\n'.join(self.documents[version])
			compressed = gzip.compress(docs)
		return compressed

	def processed_returns(self):
		""" An iterator that yields aggregated XML files and their version """ 
		if source:
			iterator = self.sql_iterator()
		else:
			iterator = self.s3_iterator()

		for index, xml_string in enumerate(iterator):
			doc, version = process_single_return(xml_string)
			self.documents[version].append(doc)

			if index % 1000:
				print(i, [{doc_ver: len(self.documents[doc_ver])} for doc_ver in self.documents])

			if len(self.documents[version]) == DOCS_PER_FILE:
				docs = process_documents_for_writing(version)
				self.documents[version] = []
				yield docs, version

	# methods that write XML to a particular source.
	# each should take same methods
	def write_s3(self, bucket, return_string, version, filename, filepath):
		filename = filepath + filename
		key = Key(bucket=self.bucket, name=filename)
		key.set_contents_from_string(return_string)

	def write_disk(self, version, return_string, filename, path):
		with open(filename , 'wb') as f: 
			f.write(return_string)

	def _get_filename(version, returns):
		filename = 'v%s_id%d.xml.gz' % (version, id(returns))
		return self.path + filename

	def run(self):
		for file_to_write, version in self.fetch_processed_returns():
			filename = _get_filename(file_to_write, version)
			if destination = 's3':
				write = write_s3
			else:
				write = write_disk
			write(files_to_write, filename)

		# write whatever is left with less than DOCS_PER_FILE
		for version in self.documents:
			returns = self.documents[version]
			filename = _get_filename(returns, version)
			write(returns, filename)



if __name__ == "__main__":
	etl = ReturnETL()
	etl.run()