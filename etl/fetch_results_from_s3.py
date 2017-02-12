import csv
import io
import re
import sys

from boto.s3.connection import S3Connection

BUCKET_NAME = 'irs-form-990-hadoop'

conn = S3Connection()
bucket = conn.get_bucket(BUCKET_NAME)

def remove_unwanted_chars_from_hadoop_output(string):
	return re.sub(r'[\[\"\]\ ]', '', string)

def fetch_results(bucket, s3_folder):
	results = []
	for key in bucket.list(prefix=s3_folder):
		if '_SUCCESS' not in key.name:
			data = key.read().decode()
			clean_data = remove_unwanted_chars_from_hadoop_output(data)
			rows = [row.split('\t') for row in clean_data.split('\n')]
			results.extend(rows)
	return results

def write_to_csv(filename, data):
	with open(filename, 'w') as f:
		writer = csv.writer(f)
		writer.writerows(data)

if __name__ == "__main__":

	if len(sys.argv) != 3:
		raise Exception("Must pass s3 prefix (directory) and output_filename")

	_, s3_folder, output_filename = sys.argv

	results = fetch_results(bucket, s3_folder)
	write_to_csv(output_filename, results)

