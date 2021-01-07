from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRBestSellingProduct(MRJob):

    def mapper_get_amounts(self, _, line):
        '''
        This mapper gets each row of the csv files,
        and then it splits into the columns (cells).
        Then it checks if the first row is not the header one,
        and then it yields the amount*quantity of each stock per year
        :param _: None
        :param line: one row from the input csv file
        :return: ((stock, year), totalamount)
        '''
        columns = re.split(r",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", line)  # splits into the columns
        if not columns[3].isalpha():  # avoid the first row of the data.
            stock = columns[1]
            quantity = float(columns[3])
            amount = float(columns[5])
            totalamount = quantity * amount
            match_year = re.search('\d{4}', columns[4])
            year = match_year.group(0)
            yield ((stock, year), totalamount)

    def reducer_sum_amounts(self, stock_year, amount):
        '''
        This reducer sums (amount*quantity) of each stock per each year
        :param stock_year: stockcode and the year
        :param amount: (amount*quantity) of the stock in that year
        :return: year, (sum(amount), stock_year)
        '''
        stock, year = stock_year
        yield year, (sum(amount), stock_year)

    def reducer_find_max_amount(self, _, data):
        '''
        This reducer finds the stock with the highest sum(amount*quantity) per each year
        :param _: year
        :param data: sum(amount*quantity) of the each stock in that year
        :return: the stock with the highest sum(amount*quantity) in a year
        '''
        yield max(data)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_amounts,
                   reducer=self.reducer_sum_amounts),
            MRStep(
                   reducer=self.reducer_find_max_amount
            )
        ]


if __name__ == '__main__':
    MRBestSellingProduct.run()