from io import BytesIO
import re
import sqlite3
import zipfile

from bs4 import BeautifulSoup
import requests

import schema

BASE_URL = 'https://www.irs.gov'
URL_WITH_LINKS_TO_YEAR_PAGES = '/charities-non-profits/current-valid-xml-schemas-and-business-rules-for-exempt-organizations-modernized-e-file'
YR_TBL_TAG_SUMMARY_REGEX = r'Links to Form 990 TY20[0-2][0-9] Schemas, Business Rules, Release Memos'

YEAR_PAGE_TABLE_NUMBER = 1
SCHEMA_TABLE_NUMBER = 1
SCHEMA_ROW_NUMBER = 1

def get_soup(url):
	r = requests.get(BASE_URL + url)
	html = r.content
	return BeautifulSoup(html, 'lxml')

def get_tax_year_pages():
	soup = get_soup(URL_WITH_LINKS_TO_YEAR_PAGES)
	table = soup.find('table')
	rows = table.find_all('tr')

	# We also want to skip the first element of rows which has no <td> elements 
	# and the last which is empty. 
	# for the remaining elements, we want to grab the first td element
	td_elements = [row.find_all('td')[0] for row in rows if row.find('td')]
	relevant_td_elements = td_elements[:-1]

	relevent_a_elements = [td.find('a') for td in relevant_td_elements]
	links = {a.text: a['href'] for a in relevent_a_elements}
	return links

def get_links_to_schema_pages(url):
	# get the versions and their links
	soup = get_soup(url)

	# get the particular table with the data we want
	tbls = soup.find_all('table')[YEAR_PAGE_TABLE_NUMBER]

	# the first row in the table is a header, the last row is blank
	rows = tbls.find_all('tr')[1:-1]
	links = [row.find('td').find('a') for row in rows]
	return {a.text: a['href'] for a in links}

def get_link_to_schema_zip(url):
	soup = get_soup(url)
	table = soup.find_all('table')[SCHEMA_TABLE_NUMBER]
	td = table.find_all('td')[SCHEMA_ROW_NUMBER]
	return BASE_URL + td.find('a')['href']

def get_schema_zip(url):
	r = requests.get(url)
	buff = BytesIO(r.content)
	return zipfile.ZipFile(buff)

def construct_row(schema_obj, key):
	""" Construct a row with a schema object and a string key, 
		representing an element in the tax return. The elements,
		in order, are:

			0: Schema version
			1: Form type (i.e. 990EZ, 990PF, etc.)
			2: Description
			3: Line number in the actual tax form
	"""
	return [schema_obj.version, 
			schema_obj.form_type, 
			schema_obj.elements[key].get('description'),
			schema_obj.elements[key].get('linenumber')]
if __name__ == "__main__":

	tax_year_pages = get_tax_year_pages()
	url = tax_year_pages['2014']

	schema_pages = get_links_to_schema_pages(url)
	version = '2014v1.0'
	url = schema_pages[version]

	link_to_schema = get_link_to_schema_zip(url)
	compressed_schema = get_schema_zip(link_to_schema)

	test_filename = '2014v1.0/TEGE/TEGE990/IRS990/IRS990.xsd'
	
	with compressed_schema.open(test_filename) as f:
		s = schema.Schema990(f, version)
	
	rows = [construct_row(s, key) for key in s.elements.keys()]

	conn = sqlite3.connect('index.db')
	cur = conn.cursor()

	CREATE_TABLE_STATEMENT = '''
	drop table if exists return_element_map;
	create table return_element_map (
		version text,
		form_type text,
		description text,
		line_number text
	);
	'''
	cur.execute(CREATE_TABLE_STATEMENT)

	INSERT_QUERY = "insert into return_element_map (version, form_type, description, line_number) values (?, ?, ?, ?)"
	cur.executemany(INSERT_QUERY, rows)

	conn.commit()











