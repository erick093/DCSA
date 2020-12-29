from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import datetime

class MRBestCustomers(MRJob):

    def mapper(self, _, line):
        columns = re.split(r",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", line)
        if not columns[3].isalpha(): # avoid the first row of the data.
            stock = columns[1]
            quantity = int(columns[3])
            match_year = re.search('\d{4}', columns[4])
            year = match_year.group(0)
            yield ((stock, year), quantity)

    def combiner(self, stock_year, quantity):
        yield (stock_year, sum(quantity))

    def reducer(self, stock_year, quantity):
        stock, year = stock_year
        yield year, (sum(quantity), stock_year)

    def reducer_aggregate(self, _, data):
        yield max(data)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(
                   reducer=self.reducer_aggregate
            )
        ]


if __name__ == '__main__':
    MRBestCustomers.run()