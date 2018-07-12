from mrjob.job import MRJob


#clase clave-valor (k,v), donde la clave es la palabra y el valor es 1
class MRCharCount(MRJob):
    def mapper(self,_,line):
        for w in line.split():
            yield w, 1
        
	#Cuantas veces se repite la palabra de la de nube generado por mapper
    def reducer(self,key,values): 
        yield key,sum(values)

if __name__ == '__main__':
    MRCharCount.run()
    
#gutenberg.com para libros
