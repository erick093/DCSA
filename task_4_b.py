from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import datetime

class MRBestCustomers(MRJob):

    def mapper(self, _, line):
        columns = re.split(r",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", line)
        if not columns[3].isalpha(): # avoid the first row of the data.
            stock = columns[1]
            amount = float(columns[5])
            match_year = re.search('\d{4}', columns[4])
            year = match_year.group(0)
            yield ((stock, year), amount)

    def combiner(self, stock_year, amount):
        yield (stock_year, sum(amount))

    def reducer(self, stock_year, amount):
        stock, year = stock_year
        yield year, (sum(amount), stock_year)

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