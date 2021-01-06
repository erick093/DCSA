from mrjob.job import MRJob
from mrjob.job import MRStep
import os

# here we open the first matrix, and we read lines, then we count the number of the rows and columns
matrix_A = open('A.txt', 'r+')
rows_matrix_A = matrix_A.readlines()
rows_no_matrix_A = len(rows_matrix_A)
cols_no_matrix_A = len(rows_matrix_A[0].split())
row_counter_A = 0

# here we open the second matrix, and we read lines, then we count the number of the rows and columns
matrix_B = open('B.txt', 'r+')
rows_matrix_B = matrix_B.readlines()
rows_no_matrix_B = len(rows_matrix_B)
cols_no_matrix_B = len(rows_matrix_B[0].split())
row_counter_B = 0

# here we open a new file named C to store the result of multiplication
matrix_C = open("C.txt", "a")

# it keeps the last key which its value is printed in the output file (it is used for organizing the output file)
row_printed_key = ''

class MRMatrixMultiplication(MRJob):

    def mapper_get_computation(self, _, line):
        '''
        This mapper gets each row of the two input matrices,
        and then it splits into the columns (cells).
        Then it checks the input filename in order to compute each matrix,
        and then it yields set of (key, value) pairs that each key has a list
        :param _: None
        :param line: one row from the input file
        :return: Set of (key, value) pairs that each key has a list
        '''
        global row_counter_A  # indicates the last iterated row number in matrix A
        global row_counter_B  # indicates the last iterated row number in matrix B
        line = line.split()  # splits the line into cells
        filename = os.environ['map_input_file']  # gets the filename from input
        if filename == 'file://A.txt':  # checks whether the input file is matrix A
            if len(line) == cols_no_matrix_A:  # checks whether the number of the cells is equal to the number of the columns
                row_counter_A += 1
                # Set of (key, value) pairs that each key has a list
                for column_counter_A in range(1, cols_no_matrix_B + 1):
                    for x in range(1, len(line) + 1):
                        yield (row_counter_A, column_counter_A), ('A', x, float(line[x - 1]))

        if filename == 'file://B.txt':  # checks whether the input file is matrix B
            if len(line) == cols_no_matrix_B:  # checks whether the number of the cells is equal to the number of the columns
                row_counter_B += 1
                # Set of (key, value) pairs that each key has a list
                for column_counter_B in range(1, rows_no_matrix_A + 1):
                    for x in range(1, len(line) + 1):
                        yield (column_counter_B, x), ('B', row_counter_B, float(line[x - 1]))

    def reducer_sort_computation(self, key, value):
        '''
        This reducer gets each (key, value) pairs,
        and then it sorts the value of each key.
        :param key: address of the value in matrix C
        :param value: values which need to be multiplied
        :return: sorted value of each key
        '''
        yield key, sorted(value)

    def mapper_get_multiplication(self, key, value):
        '''
        This mapper gets sorted (key, value) pairs,
        and then it multiplies the values. It also stores the multiplied values in matrix C
        :param key: address of the value in matrix C
        :param value: sorted values which need to be multiplied
        :return: key, multiplied result
        '''

        global row_printed_key  # the last key which its value is printed in the output file
        result = 0  # it stores the result of multiplication
        for i in range(0, cols_no_matrix_A):
            result += value[i][2] * value[i + cols_no_matrix_A][2]  # multiplication

        # if the last key is empty or the same as current key, it writes the result at the same line
        if row_printed_key == '' or row_printed_key == key[0]:
            matrix_C.write(str(result) + ' ')
        # if the last key is not equal to the current key, it writes the result at the next line
        elif row_printed_key != key:
            matrix_C.write('\r' + str(result) + ' ')
        row_printed_key = key[0]  # keeps the last iterated key

        yield key, result

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_computation,
                   reducer=self.reducer_sort_computation),
            MRStep(mapper=self.mapper_get_multiplication)
        ]

if __name__ == '__main__':
    MRMatrixMultiplication.run()

