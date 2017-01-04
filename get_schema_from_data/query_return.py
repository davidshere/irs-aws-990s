from lxml import etree
from lxml.etree import _Element

RETURN_FILENAME = 'minified_1.xml'



class TaxReturn(object):
	def __init__(self, lxml_root):
		self.root = lxml_root
		self.return_version = lxml_root.get('returnVersion')
		self.header, self.data = self.root
		self.paths = []

	def get_return_parts(self):
		""" Split the Return into sections - `IRS990EZ`, `IRS990ScheduleA`, etc... """
		# turn {http://www.irs.gov/efile}IRS990EZ) into IRS990EZ
		return {data.tag.split('}')[1]: data.getchildren() for data in self.data}

	def get_form_kind(self):
		parts = [part for part in self.get_return_parts() if 'Schedule' not in part][0]
		return parts

	def parse_paths(self, element=None, parents=[]):
		if element is None:
			element = self.root
		for elem in element.getchildren():
			elem_tag = elem.tag.split('}')[1]
			if elem.getchildren():
				self.parse_paths(elem, parents + [elem_tag])
			else:
				self.paths.append('.'.join(parents + [elem_tag]))

	def get_paths(self):
		self.parse_paths()
		return self.return_version, self.paths



with open(RETURN_FILENAME, 'r') as f:
	data = f.read()

returns = data.split('\n')

def mapper(xml):
    s = TaxReturn(xml)
    version, paths = s.get_paths()
    form_kind = s.get_form_kind()

    for path in iter(set(paths)):
        print((version, form_kind), path)


for i in returns:
    root = etree.XML(i)
    print()
    mapper(root)
    break
	
#print(len(doc.get_paths()[1]))
