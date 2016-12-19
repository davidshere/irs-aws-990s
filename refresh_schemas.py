import sqlite3

import requests

conn = sqlite3.connect(':memory:')
cur = conn.cursor()

CREATE_TABLE_STMT = 'create table schemas_990 (form_type text, version text, description text, line_number text);'
cur.execute(CREATE_TABLE_STMT)

BASE_URL = 'https://www.irs.gov/charities-non-profits/current-valid-xml-schemas-and-business-rules-for-exempt-organizations-modernized-e-file'

r = requests.get(BASE_URL)
