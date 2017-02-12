from lxml import etree

from mrjob.job import MRJob

from tax_return import TaxReturn

class MRGetFields(MRJob):

    def mapper_init(self):
        query_filename = self.options.paths
        with open(query_filename, 'r') as f:
            self.queries = f.read().split('\n')

    def mapper(self, _, line):
        tree = etree.XML(line)
        s = TaxReturn(tree)
        object_id = s.find('ObjectID')[0].text
        yield str(len((self.queries))), 1
        
    def reducer(self, key, values):
        yield key, sum(values)

    def configure_options(self):
        super(MRGetFields, self).configure_options()
        self.add_file_option('--paths')


if __name__ == '__main__':
    MRGetFields.run()