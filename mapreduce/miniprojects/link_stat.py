from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from lxml import etree
import mwparserfromhell
from heapq import *
import random
import math

def heapsort(iterable):
    h = []
    for value in iterable:
        heappush(h,value)
    return [heappop(h) for i in range(len(h))]

        
WORD_RE = re.compile(r"[\w]+")

class MRMostUsedWord(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper_init = self.mapper_init_get_page,
                   mapper = self.mapper_get_page), 
            MRStep(mapper = self.mapper_get_links,
                   reducer_init = self.reducer_init_count_links,
                   reducer = self.reducer_count_links,
                   reducer_final = self.reducer_final_count_links)
        ]

    def mapper_init_get_page(self):
        self.page_single = []
        self.page_status = 0
        
    def mapper_get_page(self, _, line):
        if '<page>' in line:
            self.page_single = []
            self.page_status = 1
            
        if self.page_status == 1:
            self.page_single.append(line)
            
        if '</page>' in line:  
            if self.page_status == 1:
                page = ''.join(self.page_single)
                root = etree.XML(page)
                content = root.xpath("//revision")[-1].xpath(".//text")[0].text
                self.page_status = 0
                if content:
                    #content = mwparserfromhell.parse(content).strip_code()
                    yield None, content
            else:
                self.page_status = 0
                self.page_single = []
            
    def mapper_get_links(self, _, line):
        links = mwparserfromhell.parse(line).filter_wikilinks()
        links = [l.encode('utf8') for l in links]
        ls = set(links)
        yield None, len(ls)
        
        
    def reducer_init_count_links(self):
        self.page_count = 0
        self.sum_link = 0
        self.sum_linksq = 0
        self.linklist = []
        
    def reducer_count_links(self, _, counts):
        for c in counts:
            self.page_count += 1
            self.sum_link += c
            self.sum_linksq += c*c
            rand = random.random()
            if rand > 0.9:
                heappush(self.linklist, c)
                #self.linklist.append(c)
        yield 0, 0

    def reducer_final_count_links(self): 
        print '\"count\" , ', self.page_count
        avg = float(self.sum_link) / self.page_count
        print '\"mean\" , ', avg
        avgsq = float(self.sum_linksq) / self.page_count
        std = math.sqrt(avgsq - avg*avg)
        print '\"stdev\" , ', std
        #li = self.linklist.sort()
        length = len(self.linklist)
        li = [ heappop(self.linklist) for i in range(length) ]
        l = float(length)
        a = int(l/4.0)
        b = int(l/2.0)
        c = int(l/4.0*3)
        print '\"25%\" , ', li[a]
        print '\"median\" , ', li[b]
        print '\"75%\" , ', li[c]
        print 'len(linklist):', length, a,b,c
        #print li

if __name__ == '__main__':
    MRMostUsedWord.run()
    
    
    
    
    
    
    