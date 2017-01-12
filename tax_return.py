from lxml import etree

RETURN_FILENAME = '200931393493000150_public.xml'

# The returns have a namespace, it needs to be included
# for querying the document to work.
IRS_NAMESPACE = 'http://www.irs.gov/efile'
IRS_NAMESPACE_PREFIX = 'irs'
NAMESPACES = {IRS_NAMESPACE_PREFIX: IRS_NAMESPACE}

FORM_TYPES = ['IRS990', 'IRS990EZ', 'IRS990PF']

class TaxReturn(object):
    def __init__(self, lxml_root):
        self.root = lxml_root
        self.return_version = lxml_root.get('returnVersion')
        if len(self.root) == 2:
            self.header, self.data = self.root
        else:
            self.header, self.data, self.object_id = self.root
        self.paths = []

    def get_return_parts(self):
        """ Split the Return into sections - `IRS990EZ`, `IRS990ScheduleA`, etc... """
        # turn {http://www.irs.gov/efile}IRS990EZ) into IRS990EZ
        return {data.tag.split('}')[1]: data.getchildren() for data in self.data}

    @property
    def form_type(self):
        if not hasattr(self, "_form_type"):
            self._form_type = [part for part in self.get_return_parts() if part in FORM_TYPES][0]
        return self._form_type

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

    def find(self, query):
        """
        Query the document for a particular node or set of nodes

        Input: 'ReturnHeader.Timestamp'
        Gets turned into the query: 'irs:ReturnHeader/irs:Timestamp'
        Which then gets run.

        Returns a list of nodes.

        """
        query_parts = query.split(".")
        query_parts = [IRS_NAMESPACE_PREFIX + ':' + tag for tag in query_parts]
        query = '/'.join(query_parts)
        return self.root.xpath(query, namespaces=NAMESPACES)


if __name__ == "__main__":

    with open(RETURN_FILENAME, 'r') as f:
        data = f.read()

    root = etree.parse(RETURN_FILENAME)
    tr = TaxReturn(root.getroot())
    print(tr.find('ReturnHeader.Timestamp'))
    print(tr.find("ReturnHeader.Filer.EIN"))
    print(tr.find("ReturnData.IRS990.Form990PartVIISectionA"))

