from datetime import datetime
import csv
import io
import json
import os

from boto.s3.connection import S3Connection
import psycopg2

BUCKET_NAME = 'irs-form-990'
INDEX_KEY_PREFIX = 'index'
INDEX_FILE_EXTENSION = '.json'

AWS_RESPONSE_JSON_FIELDS = ['DLN', 'OrganizationName', 'EIN', 'FormType', 'TaxPeriod', 'SubmittedOn', 'LastUpdated', 'ObjectId', 'URL']

def create_return_index_table(conn):
	""" Drop and create the return index """
	cur = conn.cursor()
	cur.execute('drop table if exists return_indices')
	cur.execute(CREATE_TABLE_QUERY)

def get_keys():
	""" Fetch index CSV files from the S3 bucket and return a list of Keys """
	conn = S3Connection()
	bucket = conn.get_bucket(BUCKET_NAME)
	index_keys = [a for a in bucket.list(prefix=INDEX_KEY_PREFIX) if INDEX_FILE_EXTENSION in a.name]
	return index_keys

def process_index_key(key):
	""" Take a Key object and return a list of lists formatted for Postgres """

	# the csv module needs a string, not bytes
	data = key.read().decode()
	records = json.loads(data)
	# the API returns a dictionary with a single key that maps to a list of dictionaries, so we need that key, to get the list
	record_key = list(records.keys())[0]
	rows = [[row[col] for col in AWS_RESPONSE_JSON_FIELDS if row[col]] for row in records[record_key]]

	for i, row in enumerate(rows):
		row.append(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
		row.append(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
		row.append('\\N') # null value for return_version since we don't have that in this raw data

	return rows

def write_index_data_to_db(data_str, conn):
	""" Take a list of lists and write each element (inner list) to the DB """
	cur = conn.cursor()
	cur.copy_from(data_str, 'return_indices')

if __name__ == "__main__":

	# connect to the database
	dbconn = psycopg2.connect(os.environ['DB_URI'])

	# fetch the keys from S3 and write their contents to the database.
	keys = get_keys()
	id_field = 0
	for key in keys:
		print(key)
		rows = process_index_key(key)
		print(len(rows))

		for row in rows:
			row.insert(0, str(id_field))
			id_field += 1

		data = '\n'.join(['\t'.join([val for val in row]) for row in rows])
		buff = io.StringIO(data)
		write_index_data_to_db(buff, dbconn)

	# commit the changes
	dbconn.commit()
