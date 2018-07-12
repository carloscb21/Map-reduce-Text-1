from mrjob.job import MRJob
import string

class MRCharCount(MRJob):
    def mapper(self,_,line):
        for x in string.punctuation:
            line = line.replace(x,' ')
            
        for word in line.split():
            if len(word) > 4:
                yield (word.lower(), 1)
        
    def reducer(self,key,values):
        suma = sum(values)
        if suma > 50:
            yield key,suma

if __name__ == '__main__':
    MRCharCount.run()
    
#gutenberg.com para libros
