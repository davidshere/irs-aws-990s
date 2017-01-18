from lxml import etree

from mrjob.job import MRJob

from tax_return import TaxReturn

class MRGetFields(MRJob):

    def mapper(self, _, line):
        tree = etree.XML(line)
        s = TaxReturn(tree)
        object_id = s.find('ObjectID')[0].text
        yield object_id, 1
        

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRGetFields.run()