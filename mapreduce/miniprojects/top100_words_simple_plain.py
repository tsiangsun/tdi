'''
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):
    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1
        
    def reducer(self, key, values):
        yield key, sum(values)
        '''

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

from heapq import *
def heapsort(iterable):
    h = []
    for value in iterable:
        heappush(h,value)
    return [heappop(h) for i in range(len(h))]

        
WORD_RE = re.compile(r"[\w]+")

class MRMostUsedWord(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]
    
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

        
        
#from heapq import *
#sconn_list = heapsort(conn_list)

#pop_list = []
#for pair in sconn_list:
#    t = (pair[1], -pair[0])
#    pop_list.append(t)
    
#pop_list[:100]


if __name__ == '__main__':
    MRMostUsedWord.run()
    
    