import os
import sys

import psycopg2

if len(sys.argv) not in (3 , 4):
	raise Exception("Must pass input filename and target table name as arguments")

if len(sys.argv) == 3:
	_, infile_name, target_table = sys.argv
	columns = None
else:
	_, infile_name, target_table, columns = sys.argv
	columns = columns.split(',')


with open(infile_name, 'r') as data:
	with psycopg2.connect(os.environ['DB_URI']) as conn:

		copy_params = {
			'file': data,
			'table': target_table,
			'sep': '|',
		}
		if columns:
			copy_params.update({'columns': columns})

		cur = conn.cursor()
		cur.copy_from(**copy_params)
		conn.commit()
