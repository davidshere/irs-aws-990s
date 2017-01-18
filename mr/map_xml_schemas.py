import os

from mrjob.job import MRJob
from lxml import etree

from tax_return import TaxReturn

class MRFindUniquePaths(MRJob):

    def mapper(self, _, line):
        tree = etree.XML(line)
        s = TaxReturn(tree)
        version = s.return_version
        paths = s.paths
        form_type = s.form_type
        for path in paths:
            yield (version, form_type), path

    def reducer(self, key, values):
        paths = set(values)
        for path in paths:
            yield key, path


if __name__ == '__main__':
    MRFindUniquePaths.run()