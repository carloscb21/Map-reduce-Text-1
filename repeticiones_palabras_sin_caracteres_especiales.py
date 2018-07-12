from mrjob.job import MRJob
import string

#clase clave-valor (k,v), donde la clave es la palabra y el valor es 1
#eliminamos las comas, las comillas, etc, y los ponemos como espacios en blanco, y el split divide en espacio en blanco

class MRCharCount(MRJob):
    def mapper(self,_,line):
        for x in string.punctuation: #obtengo los caracteres con signos de puntuacion
            line = line.replace(x,' ') #los remplazo por vacio los signos de puntuacion
        for word in line.split():
            #coge palabras de mayor longitud, sin el len(word) coge todas
            if len(word)>4:
                yield (word.lower(), 1)
        
    def reducer(self,key,values):
        yield key,sum(values)

if __name__ == '__main__':
    MRCharCount.run()
    
#gutenberg.com para libros
