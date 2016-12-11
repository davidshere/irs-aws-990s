import csv
import io
import sqlite3

from boto.s3.connection import S3Connection

BUCKET_NAME = 'irs-form-990'
INDEX_KEY_PREFIX = 'index_2014'
CSV_EXTENSION = '.csv'

INDEX_TABLE_NAME = 'return_index'
SQLITE_DB = 'index.db'

CHECK_FOR_TABLE_QUERY = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;"
CREATE_TABLE_QUERY = '''
	create table return_index (
		return_id text,
		filing text,
		ein integer,
		tax_period text,
		sub_date text,
		taxpayer_name text,
		return_type text,
		dln text,
		object_id text
	);
'''
INSERT_QUERY = "insert into return_index (return_id, filing, ein, tax_period, sub_date, taxpayer_name, return_type, dln, object_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?)"

def create_or_truncate_return_index_table(conn):
	""" Check for the existence of the return_index table in a SQLite3 DB """
	cur = conn.cursor()
	cur.execute(CHECK_FOR_TABLE_QUERY, [INDEX_TABLE_NAME])
	if cur.fetchall():
		cur.execute('delete from return_index')
	else:
		cur.execute(CREATE_TABLE_QUERY)

def get_keys():
	""" Fetch index CSV files from the S3 bucket and return a list of Keys """
	conn = S3Connection()
	bucket = conn.get_bucket(BUCKET_NAME)
	index_keys = [a for a in bucket.list(prefix=INDEX_KEY_PREFIX) if CSV_EXTENSION in a.name]
	return index_keys

def process_index_key(key):
	""" Take a Key object and return a list of lists formatted for the SQLite3 DB """

	# the csv module needs a string, not bytes
	data = key.read().decode()
	buff = io.StringIO(data)
	reader = csv.reader(buff)
	rows = [row for row in reader]
	import pdb
	pdb.set_trace()
	
	# don't need the final newline or the header row
	rows.pop(0)
	return rows

def write_index_data_to_db(index_rows, conn):
	""" Take a list of lists and write each element (inner list) to the DB """
	cur = conn.cursor()
	cur.executemany(INSERT_QUERY, index_rows)

if __name__ == "__main__":

	# connect to the database
	dbconn = sqlite3.connect(SQLITE_DB)

	# create the table if it doesn't exist, truncate it if it does
	create_or_truncate_return_index_table(dbconn)

	# fetch the keys from S3 and write their contents to the database.
	keys = get_keys()
	for key in keys:
		print(key)
		data = process_index_key(key)
		write_index_data_to_db(data, dbconn)

	# commit the changes
	dbconn.commit()
