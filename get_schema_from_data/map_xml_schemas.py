from mrjob.job import MRJob
from lxml import etree

from query_return import TaxReturn


class MRFindUniquePaths(MRJob):

    def mapper(self, _, line):
        tree = etree.XML(line)
        s = TaxReturn(tree)
        version, paths = s.get_paths()
        form_kind = s.get_form_kind()
        for path in paths:
            yield (version, form_kind), path


    def reducer(self, key, values):
        paths = set(values)
        for path in paths:
            yield key, path


if __name__ == '__main__':
    MRFindUniquePaths.run()