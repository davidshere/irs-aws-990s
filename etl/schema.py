from collections import defaultdict
from lxml import etree

SCHEMA_FILENAME = "efile990x_2015v2.1/2015v2.1/TEGE/Common/IRS990ScheduleB/IRS990ScheduleB.xsd"

XML_ELEMENT_TAG = '{http://www.w3.org/2001/XMLSchema}element'

XML_TAGS = {
	'{http://www.w3.org/2001/XMLSchema}schema': 'schema',
	'{http://www.w3.org/2001/XMLSchema}annotation': 'annotation',
	'{http://www.w3.org/2001/XMLSchema}documentation': 'documentation',
	XML_ELEMENT_TAG: 'element',
	'{http://www.w3.org/2001/XMLSchema}complexType': 'complex_type',
	'{http://www.w3.org/2001/XMLSchema}complexContent': 'complex_content',
	'{http://www.w3.org/2001/XMLSchema}extension': 'extension',
	'{http://www.w3.org/2001/XMLSchema}attributeGroup': 'attribute_group',
	'{http://www.w3.org/2001/XMLSchema}attribute': 'attribute',
	'{http://www.w3.org/2001/XMLSchema}sequence': 'sequence',
	'{http://www.w3.org/2001/XMLSchema}group': 'group',
	'{http://www.w3.org/2001/XMLSchema}choice': 'choice',
	'{http://www.w3.org/2001/XMLSchema}simpleContent': 'simple_content',
	'{http://www.w3.org/2001/XMLSchema}simpleType': 'simple_type',
	'{http://www.w3.org/2001/XMLSchema}restriction': 'restriction',
	'{http://www.w3.org/2001/XMLSchema}pattern': 'pattern',
	'{http://www.w3.org/2001/XMLSchema}totalDigits': 'total_digits',
	'{http://www.w3.org/2001/XMLSchema}maxLength': 'max_length',
	'{http://www.w3.org/2001/XMLSchema}include': 'include',
	'{http://www.w3.org/2001/XMLSchema}fractionDigits': 'fraction_digits',
	'{http://www.w3.org/2001/XMLSchema}minInclusive': 'min_inclusive',
	'{http://www.w3.org/2001/XMLSchema}maxInclusive': 'max_inclusive',
	'{http://www.w3.org/2001/XMLSchema}enumeration': 'enumeration'
}

IRS_TAGS = {
	'{http://www.irs.gov/efile}Description': 'description',
	'{http://www.irs.gov/efile}LineNumber': 'line_number',
	'{http://www.irs.gov/efile}TaxYear': 'tax_year',
	'{http://www.irs.gov/efile}MaturityLevel': 'maturity_level',
	'{http://www.irs.gov/efile}ReleaseDate': 'release_date'
}

class Schema990:
	""" A class to convert IRS XSD documents into tabular form.

		Takes a filename as input and parses the tree on instantiation.
	"""
	def __init__(self, filename, version):
		# create the basic tree objects
		self.tree = etree.parse(filename)
		self.root = self.tree.getroot()

		# parse the tree
		self.elements = defaultdict(dict)
		self.parse(self.root)
		# this shouldn't still be a defaultdict
		self.elements = dict(self.elements)
		
		self.form_type = self.get_form_type()
		self.version = version

	def parse(self, elem, element_type=None, verbose=False):
		""" We need to recursively parse an XML schema tree with two different namespaces.
			One namespace is used by the XML schema definition language, the other by the IRS

			element_type is used to keep track of the name of the element as we move down the tree to the actual content
			verbose tells the function whether or not to print information to the screen as it progresses
		"""
		# get the elements children, and remove comments
		children = elem.getchildren()
		children_without_comments = [child for child in children if not isinstance(child, etree._Comment)]

		# iterate through the children
		# if there's another child element, parse that, otherwise move on to the sibling
		for child in children_without_comments:

			# is it an XML tag?
			if child.tag in XML_TAGS.keys():
				current_element = None
				tag_type = XML_TAGS.get(child.tag)

				# is it an element? if so, it'll have a name that we want to 
				# keep track of as we're moving recursively down the tree
				if tag_type == 'element' and child.get('name'):
					current_element = child.get('name')
					if verbose:
						print("Element type:", current_element)

				# if we have a passed element name or we got an element name, we
				# want to call the parser with that element name. Otherwise we 
				# call it without out.
				if current_element:
					self.parse(child, element_type=current_element)
				elif element_type:
					self.parse(child, element_type=element_type)
				else:
					self.parse(child)
			
			# or is it an IRS tag?
			elif child.tag in IRS_TAGS.keys():
				self.elements[element_type][self._get_element_tag_without_namespace(child)] = child.text

			# otherwise notify the user that something is going on
			else:
				print('Unidentified tag:', child.tag)

	def get_form_type(self):
		top_level_element_tag = [elem for elem in self.root if elem.tag == XML_ELEMENT_TAG][0]
		return top_level_element_tag.get('name')

	def _get_element_tag_without_namespace(self, elem):
		""" Returns the element tag without the corresponding namespace 

			:param elem: The element from which we want to extract tag information
			:type elem: an etree.Element object
			:rtype: str
		"""
		return elem.tag.split('}')[1].lower()

	def get(self, element_name):
		if element_name in self.elements.keys():
			return self.elements[element_name]
		else:
			return None

	def __str__(self):
		return "<IRS Schema Form: %s Version: %s>" % (self.form_type, self.version)

	def __repr__(self):
		return "<IRS Schema Form: %s Version: %s>" % (self.form_type, self.version)


if __name__ == "__main__":
	schema = Schema990(SCHEMA_FILENAME, '2015v2.1')


