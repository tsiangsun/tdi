from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from lxml import etree
import mwparserfromhell
from heapq import *
def heapsort(iterable):
    h = []
    for value in iterable:
        heappush(h,value)
    return [heappop(h) for i in range(len(h))]

        
WORD_RE = re.compile(r"[\w]+")
#page_single = []

class MRMostUsedWord(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper_init = self.mapper_init_get_page,
                   mapper = self.mapper_get_page), 
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
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
            #self.page_single.append(line)  
            if self.page_status == 1:
                page = ''.join(self.page_single)
                root = etree.XML(page)
                content = root.xpath("//revision")[-1].xpath(".//text")[0].text
                self.page_status = 0
                if content:
                    content = mwparserfromhell.parse(content).strip_code()
                    yield None, content
            else:
                self.page_status = 0
                self.page_single = []


    #def mapper_final_get_page(self, _, page):
        
        
    def mapper_get_words(self, _, line):
        # yield each word in the line
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)
            
    def combiner_count_words(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))
        
    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), word)
        
    # discard the key; it is just None
    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=
        h = []
        for pair in word_count_pairs:
            heappush(h, (-pair[0], pair[1]) )
            
        result = [heappop(h) for i in range(100)]
        for pair in result:
            yield (pair[1], -pair[0])

        


if __name__ == '__main__':
    MRMostUsedWord.run()
    
    