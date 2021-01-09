from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRBestSellingProduct(MRJob):

    def mapper_get_quantities(self, _, line):
        '''
        This mapper gets each row of the csv files,
        and then it splits into the columns (cells).
        Then it checks if the first row is not the header one,
        and then it yields the quantity of each stock per year
        :param _: None
        :param line: one row from the input csv file
        :return: ((stock, year), quantity)
        '''
        columns = re.split(r",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", line)  # splits into the columns
        if not columns[3].isalpha():  # avoid the first row of the data.
            stock = columns[1]
            quantity = int(columns[3])
            match_year = re.search('\d{4}', columns[4])
            year = match_year.group(0)
            yield ((stock, year), quantity)

    def reducer_sum_quantities(self, stock_year, quantity):
        '''
        This reducer sums the quantity of each stock per each year
        :param stock_year: stockcode and the year
        :param quantity: quantity of the stock in that year
        :return: year, (sum(quantity), stock_year)
        '''
        stock, year = stock_year
        yield year, (sum(quantity), stock)

    def reducer_find_max_quantity(self, _, data):
        '''
        This reducer finds the stock with the highest quantity per each year
        :param _: year
        :param data: sum(quantity) of the each stock in that year
        :return: the stock with the highest quantity in a year
        '''
        yield max(data)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_quantities,
                   reducer=self.reducer_sum_quantities),
            MRStep(
                   reducer=self.reducer_find_max_quantity
            )
        ]


if __name__ == '__main__':
    MRBestSellingProduct.run()