from lxml import etree

from mrjob.job import MRJob

from tax_return import TaxReturn

class MRGetFields(MRJob):
    """ Requires a --paths options pointing to a file where
        each row is a proper dot-separated query string

        TODO:   Update this to include version information -
                No point querying strings that don't exist
                in that version!
    """

    def mapper_init(self):
        query_filename = self.options.paths
        with open(query_filename, 'r') as f:
            self.queries = f.read().split('\n')

    def mapper(self, _, line):
        tree = etree.XML(line)
        s = TaxReturn(tree)
        object_id = s.find('ObjectID')[0].text
        for query in self.queries:
            results = s.find(query)
            for result in results:
                result_text = result.text
                yield (object_id, query), result_text

    def configure_options(self):
        super(MRGetFields, self).configure_options()
        self.add_file_option('--paths')


if __name__ == '__main__':
    MRGetFields.run()