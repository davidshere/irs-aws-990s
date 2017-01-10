from lxml import etree

from mrjob.job import MRJob

from query_return import TaxReturn

class MRGetFields(MRJob):

    def mapper(self, _, line):
        tree = etree.XML(line)
        s = TaxReturn(tree)
        

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRGetFields.run()