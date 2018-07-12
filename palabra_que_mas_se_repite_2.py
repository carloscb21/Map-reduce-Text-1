from mrjob.job import MRJob
from mrjob.step import MRStep
import string


class MRCharCount(MRJob):
    def mapper(self,_,line):
        for x in string.punctuation:
            line = line.replace(x,' ')
            
        for word in line.split():
            yield (word.lower(), 1)
    #primer reducer, lo que emita, se va al segundo reducer(reducer)
    def sum_words(self, word, counts):
        yield None, (sum(counts), word)
    
    #segundo reducer
    def reducer(self,key,values):
        max = 0
        max_word = ''
        for (counts, word) in values:
            if counts > max:
                max = counts
                max_word = word
        yield max_word,max
    
    #los pasos
    def steps(self):
        return [MRStep(mapper = self.mapper, reducer = self.sum_words),MRStep(reducer= self.reducer)]

if __name__ == '__main__':
    MRCharCount.run()
    
#gutenberg.com para libros
