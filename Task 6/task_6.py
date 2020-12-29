from mrjob.job import MRJob
from mrjob.job import MRStep
import numpy as np
import os

inputfile = open('A.txt', 'r+')
linesOfFile = inputfile.readlines()
# noOfRows, _ = linesOfFile[0].split()

inputfile2 = open('B.txt', 'r+')
linesOfFile2 = inputfile2.readlines()
# _, noOfCols = linesOfFile2[0].split()


noOfRows = len(linesOfFile)
column_first = len(linesOfFile[0].split())
rows_second = len(linesOfFile2)
noOfCols = len(linesOfFile2[0].split())


row_counter_A = 0
row_counter_B = 0


class MatrixMultiplication(MRJob):
    #f = open('OutputAlgorithmB.txt', 'w')

    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                 reducer=self.reducer1),
            MRStep(mapper=self.mapper2)
        ]

    def mapper1(self,_,line):
        global row_counter_A
        global row_counter_B
        line = line.split()
        filename = os.environ['map_input_file']
        if filename == 'file://A.txt':
            if len(line) == column_first:   # Martix A
                row_counter_A+=1
                for column_counter_A in range(1,noOfCols+1):
                    for x in range(1,len(line)+1):
                        yield (row_counter_A,column_counter_A),('A',x,float(line[x-1]))

        if filename == 'file://B.txt':
            if len(line) == noOfCols:    #Matrix B
                row_counter_B+=1
                for row_counter in range(1,noOfRows+1):
                    for x in range(1,len(line)+1):
                        yield (row_counter,x),('B',row_counter_B,float(line[x-1]))

    def reducer1(self,key,value):
        yield key,sorted(value)

    def mapper2(self, key, value):
        result = 0
        for i in range(0, column_first):
            result += value[i][2] * value[i + column_first][2]
        yield key, result

if __name__ == '__main__':
    MatrixMultiplication.run()

