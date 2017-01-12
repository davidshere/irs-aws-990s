import unittest

from lxml import etree

import tax_return as tr

TEST_FILENAME = 'sample_return_1.xml'

class TestTaxReturn(unittest.TestCase):

	def setUp(self):
		self.filename = TEST_FILENAME
		with open(self.filename, 'rb') as f:
			self.input_str = f.read()
		self.root = etree.XML(self.input_str)
		self.doc = tr.TaxReturn(self.root)

	def test_init(self):
		""" Test the instantiation of the object """
		self.assertTrue(isinstance(self.doc, tr.TaxReturn))

	def test_version(self):
		return_version = self.doc.return_version
		self.assertEqual(return_version, '2008v2.7')

	def test_return_parts(self):
		expected = ['IRS990', 'IRS990ScheduleB', 'IRS990ScheduleD', 'IRS990ScheduleO']
		found = self.doc._get_return_parts()
		self.assertEqual(found, expected)

	def test_form_type(self):
		expected = 'IRS990'
		found = self.doc.form_type
		self.assertEqual(found, expected)

	def test_get_paths(self):
		""" Testing that we get the number of paths we expect to and that 
			a sample of their values are what we expect. """
		paths = self.doc.paths
		found = len(paths)
		self.assertEqual(605, found)
		self.assertEqual(paths[0], 'ReturnHeader.Timestamp')
		self.assertEqual(paths[-1], 'ReturnData.IRS990ScheduleO.GeneralExplanation.Explanation')
		self.assertEqual(paths[10], 'ReturnHeader.Filer.Name.BusinessNameLine1')
		
	def test_find(self):
		ts = self.doc.find('ReturnHeader.Timestamp')[0]
		self.assertEqual(ts.tag, '{http://www.irs.gov/efile}Timestamp')
		
		ein = self.doc.find("ReturnHeader.Filer.EIN")[0]
		self.assertEqual(ein.tag, '{http://www.irs.gov/efile}EIN')

		section_a = self.doc.find("ReturnData.IRS990.Form990PartVIISectionA")
		self.assertEqual(len(section_a), 47)


if __name__ == "__main__":
	unittest.main()