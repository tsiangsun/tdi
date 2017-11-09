from mrjob.job import MRJob, MRStep

from lxml import etree
import mwparserfromhell

import heapq

import re

from math import log,sqrt
from random import randint

parselink = re.compile(r'\[\[(.*?)(\]\]|\|)')

parser = etree.XMLParser(recover = True)

class linkstats(MRJob):

    def __init__(self, args=None):
        self._chunk = ''
        super(linkstats, self).__init__(args)

    def mapper(self, _, line):
        try:
            self._chunk += line.strip()
            if re.search(r"</page>", line):
                text = ''
                self._slurping = False
                root = etree.fromstring(self._chunk, parser)
                texts = root and root.xpath('//text')
                if texts:
                    text = texts[0].text
                if text:
                    lset = set()
                    mwp = mwparserfromhell.parse(text)
                    links = mwp.filter_wikilinks()
                    for link in links:
                        match = parselink.search(unicode(link))
                        lset.add(match.groups()[0])
                    yield None, len(lset)
                self._chunk = ''
        except:
            self._chunk = ''

    def reducer(self,_,counts):
        R_SIZE = 1000000
        reservoir = []
        c = 0
        S = 0
        SS = 0
        for count in counts:
            S += count
            SS += (count * count)
            if c < R_SIZE:
                reservoir.append(count)
            else:
                r = randint(0,c)
                if r < R_SIZE:
                    reservoir[r] = count
            c += 1
        R_SIZE = min(R_SIZE,c)
        mean = float(S) / float(c)
        stddev = sqrt((float(SS) / float(c)) - (mean * mean))
        reservoir = sorted(reservoir)
        q05 = reservoir[int(R_SIZE * .05)]
        q25 = reservoir[int(R_SIZE * .25)]
        q50 = reservoir[int(R_SIZE * .50)]
        q75 = reservoir[int(R_SIZE * .75)]
        q95 = reservoir[int(R_SIZE * .95)]
        yield 'count', c
        yield 'mean', mean
        yield 'stddev', stddev
        yield 'q05', q05
        yield 'q25', q25
        yield 'q50', q50
        yield 'q75', q75
        yield 'q95', q95

    

if __name__ == '__main__':
    linkstats.run()