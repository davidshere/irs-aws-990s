import io
import gzip

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from lxml import etree

SOURCE_BUCKET = 'irs-form-990'
DEST_BUCKET = 'irs-form-990-hadoop'
MAX_BYTES_PER_FILE = 67108864

RETURN_SET_WRAPPER_TAG_SIZE = 0

conn = S3Connection()

source_bucket = conn.get_bucket(SOURCE_BUCKET)
dest_bucket = conn.get_bucket(DEST_BUCKET)

def clean_xml_string(string):
	""" We want to get rid of newlines and excess whitespace """
	without_newlines = string.replace(b'\\r\\n', b'')
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

	object_id = item.name.split('_')[0]
	single_return.append(etree.XML('<ObjectID>%s</ObjectID>' % object_id))

	compressed_return = gzip.compress(etree.tostring(single_return))
	item_size = len(compressed_return)
	return compressed_return, item_size


def fetch_processed_returns():
	""" Fetches XML files from S3, does some transformation,
		combines them until they reach a certain size, and then
		yields the lxml._Element.
	"""
	fp = io.BytesIO(RETURN_SET_OPEN_TAG)
	file_size = RETURN_SET_WRAPPER_TAG_SIZE

	with open('object_ids.csv', 'r') as f:
		list_of_existing_returns = f.read().split('\n')
		filenames = ['{object_id}_public.xml'.format(object_id=object_id) for object_id in list_of_existing_returns]
		print(filenames[:10])

	for i, item in enumerate(source_bucket.list()):
		if i % 5000 == 0: 
			print(i)
		fname = item.name
		if fname.endswith('_public.xml') and fname not in filenames:
			compressed_return, item_size = process_single_return(item)
			# add the return to the set if the file is small enough, otherwise 
			# yield the set and reset it

			if file_size + item_size < MAX_BYTES_PER_FILE:
				fp.write(compressed_return)
				file_size += item_size
			else:
				# need to yield the return set and set up a next one
				fp.write(RETURN_SET_CLOSED_TAG)
				old_fp = fp

				fp = io.BytesIO(RETURN_SET_OPEN_TAG)
				fp.write(compressed_return)
				file_size = RETURN_SET_WRAPPER_TAG_SIZE + item_size

				yield old_fp

def upload_files_to_s3(bucket, fp):
	filename = 'xml-files/%d.xml.gz' % id(fp)
	key = Key(bucket=bucket, name=filename)
	key.set_contents_from_file(fp, rewind=True)

def write_files_to_disk(returns):
	for i, return_set in enumerate(returns):
		print(i, return_set)
		with open('spark-test/xml/%d.xml.gz' % id(i), 'wb') as f:
			f.write(etree.tostring(return_set))

if __name__ == "__main__":
	for returns in fetch_processed_returns():
		upload_files_to_s3(dest_bucket, returns)