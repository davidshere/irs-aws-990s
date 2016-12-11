import csv
import io
import json
import sqlite3

from boto.s3.connection import S3Connection

BUCKET_NAME = 'irs-form-990'
INDEX_KEY_PREFIX = 'index'
INDEX_FILE_EXTENSION = '.json'

AWS_RESPONSE_JSON_FIELDS = ['DLN', 'OrganizationName', 'EIN', 'FormType', 'TaxPeriod', 'SubmittedOn', 'LastUpdated', 'ObjectId', 'URL']
SQLITE_DB = 'index.db'

CREATE_TABLE_QUERY = '''
	create table return_index (
		dln text,
		org_name text,
		ein text,
		form_type text,
		tax_period text,
		sub_date text,
		last_updated,
		object_id text,
		url text
	);
'''

INSERT_QUERY = "insert into return_index (dln, org_name, ein, form_type, tax_period, sub_date, last_updated, object_id, url) values (?, ?, ?, ?, ?, ?, ?, ?, ?)"

def create_return_index_table(conn):
	""" Check for the existence of the return_index table in a SQLite3 DB """
	cur = conn.cursor()
	cur.execute('drop table if exists return_index')
	cur.execute(CREATE_TABLE_QUERY)

def get_keys():
	""" Fetch index CSV files from the S3 bucket and return a list of Keys """
	conn = S3Connection()
	bucket = conn.get_bucket(BUCKET_NAME)
	index_keys = [a for a in bucket.list(prefix=INDEX_KEY_PREFIX) if INDEX_FILE_EXTENSION in a.name]
	return index_keys

def process_index_key(key):
	""" Take a Key object and return a list of lists formatted for the SQLite3 DB """

	# the csv module needs a string, not bytes
	data = key.read().decode()
	records = json.loads(data)
	# the API returns a dictionary with a single key that maps to a list of dictionaries, so we need that key, to get the list
	record_key = list(records.keys())[0]
	rows = [[row[col] for col in AWS_RESPONSE_JSON_FIELDS] for row in records[record_key]]

	
	return rows

def write_index_data_to_db(index_rows, conn):
	""" Take a list of lists and write each element (inner list) to the DB """
	cur = conn.cursor()
	cur.executemany(INSERT_QUERY, index_rows)

if __name__ == "__main__":

	# connect to the database
	dbconn = sqlite3.connect(SQLITE_DB)

	# create the table, dropping an existing table if one exists
	create_return_index_table(dbconn)

	# fetch the keys from S3 and write their contents to the database.
	keys = get_keys()
	for key in keys:
		print(key)
		data = process_index_key(key)
		write_index_data_to_db(data, dbconn)

	# commit the changes
	dbconn.commit()
