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

		source - 'boto' if you're going to iterate through the contents
				 of `source_bucket`, anything else if you want to get 
				 URLs from the DB and iterate through those.

		destination - 's3' if you're writing output to S3, otherwise you'll
					  be writing to disk.

		style - 'line-delimited' if you want line-delimited xml strings. That's
				the only functionality for now. The alternative would be to write
				files that are valid XML.

		dest_path - the path, relative or absolute, that you want to write your
					output to, regardless of whether that's s3 or disk

		source_bucket - Necessary if source='boto'. This is the bucket
						through which you'll iterate. The keys should be
						XML files that end with '_public.xml'.

		dest_bucket - Necessary if destination='s3'. 

		query - Necessary if source != 'boto'. This should return a single
				column with urls pointing to the XML files you want to pull.
	"""
	def __init__(self, 
				 source='boto', 
				 destination='s3', 
				 style='line-delimited',
				 dest_path = '.',
				 source_bucket=None,
				 dest_bucket=None,
				 query=None):
		# these attributes are used regardless of source or destination
		self.documents = defaultdict(list)
		self.source = source
		self.destination = destination
		self.style = style
		self.dest_path = dest_path

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

		object_id = item.name.split('_')[0]
		single_return.append(etree.XML('<ObjectID>%s</ObjectID>' % object_id))

		version = single_return.get('returnVersion')
		single_return = etree.tostring(single_return)
		return single_return, version

	## iterators to that return raw xml for processing
	def sql_iterator(self):
		conn = psycopg2.connect(os.environ['DB_URI'])
		cur = conn.cursor()
		cur.execute(self.query)
		for result in cur:
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
		""" Turns a list of XML strings into a single gzipped and
			line delimited document 
		"""
		if self.style=='line-delimited':
			docs = b'\n'.join(self.documents[version])
			compressed = gzip.compress(docs)
		return compressed

	def processed_returns(self):
		""" An iterator that yields aggregated XML files and their version """ 
		if self.source:
			iterator = self.sql_iterator()
		else:
			iterator = self.s3_iterator()

		for index, xml_string in enumerate(iterator):
			doc, version = process_single_return(xml_string)
			self.documents[version].append(doc)

			if index % 100:
				print(i, [{doc_ver: len(self.documents[doc_ver])} for doc_ver in self.documents])

			if len(self.documents[version]) == DOCS_PER_FILE:
				docs = process_documents_for_writing(version)
				self.documents[version] = []
				yield docs, version

	# methods that write XML to a particular source.
	# each should take same methods
	def write_s3(self, return_string, filepath):
		key = Key(bucket=self.dest_bucket, name=filepath)
		key.set_contents_from_string(return_string)

	def write_disk(self, return_string, filepath):
		with open(filepath , 'wb') as f: 
			f.write(return_string)

	def _get_filename(self, returns, version):
		filename = 'v%s_id%d.xml.gz' % (version, id(returns))
		return self.path + filename

	def run(self):
		for file_to_write, version in self.processed_returns():
			filename = _get_filename(file_to_write, version)
			if destination == 's3':
				write = write_s3
			else:
				write = write_disk
			path = _get_filename(file_to_write, version)
			write(files_to_write, filename)

		# write whatever is left with less than DOCS_PER_FILE
		for version in self.documents:
			returns = self.documents[version]
			path = _get_filename(returns, version)
			write(returns, path)



if __name__ == "__main__":

	urls_to_fetch_query = 'select url from return_indices where object_id not in (select object_id from found_ids);'

	etl = ReturnETL(source='sql',
					destination='s3',
					dest_bucket=DEST_BUCKET,
					query=urls_to_fetch_query,
					dest_path=S3_FOLDER)
	etl.run()