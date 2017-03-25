import csv
import io
import re
import sys

from boto.s3.connection import S3Connection

""" Fetches data from Amazon S3 and writes it to a `|`-separated
	file.

	You have to input directory on S3 (bucket is hard-coded for now)
	As well as a filename to dump the output.

	The input should be what MRJob returns from EMR
"""

BUCKET_NAME = 'irs-form-990-hadoop'

conn = S3Connection()
bucket = conn.get_bucket(BUCKET_NAME)

def remove_unwanted_chars_from_hadoop_output(hadoop_row):
	return re.sub(r'[-%\[\"\]\']', '', hadoop_row)

def fetch_results(bucket, s3_folder):
	results = []
	for key in bucket.list(prefix=s3_folder):
		print("Fetching %s" % key)
		if '_SUCCESS' not in key.name:
			data = key.read().decode()
			clean_data = remove_unwanted_chars_from_hadoop_output(data)
			rows = [row.split('\t') for row in clean_data.split('\n')]
			results.extend(rows)
	return results

def write_to_csv(filename, data, sep='|'):
	with open(filename, 'w') as f:
		write_str = '\n'.join([sep.join(row) for row in data if any(row)])
		f.write(write_str)

if __name__ == "__main__":

	if len(sys.argv) != 3:
		raise Exception("Must pass s3 prefix (directory) and output_filename")

	_, s3_folder, output_filename = sys.argv

	results = fetch_results(bucket, s3_folder)
	write_to_csv(output_filename, results)

