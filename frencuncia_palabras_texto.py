from mrjob.job import MRJob
from mrjob.step import MRStep
import string


class MRCharCount(MRJob):
    SORT_VALUES = True    
    
    def mapper(self,_,line):
        for x in string.punctuation:
            line = line.replace(x,' ')
        words = line.split()
        for word in words:
            yield (word.lower(), 1)
        yield '.total_counter.',len(words)

    #primer reducer, lo que emita, se va al segundo reducer(reducer)
    def sum_words(self, word, counts):
        yield None, (sum(counts), word)
    
    #segundo reducer
    def frequency(self,_,data):
        #first_value = data.next()
	
	data_list = list(data)
	total = 0
        for counts, word in data_list:
            if word == ".total_counter.":
                total = float(counts)
	print(total)
        for counts, word in data_list:
            yield word,float(counts)/total
    
    #los pasos
    def steps(self):
        return [MRStep(mapper = self.mapper, reducer = self.sum_words),MRStep(reducer= self.frequency)]

if __name__ == '__main__':
    MRCharCount.run()
    
#gutenberg.com para libros
