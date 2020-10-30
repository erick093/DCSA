from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRBestCustomers(MRJob):

    def mapper(self, _, line):
        columns = re.split(r",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", line)
        if not columns[3].isalpha():
            c_id = columns[6]
            amount = int(columns[3])*float(columns[5])
            match_year = re.search('\d{4}', columns[4])
            year = match_year.group(0)
            yield ((c_id, year), amount)

    def combiner(self, id_year, amount):
        yield id_year, sum(amount)

    def reducer(self, id_year, amount):
        c_id, year = id_year
        total_amount = sum(amount)
        yield year, (c_id, total_amount)

    def reducer_aggregate(self, year, data):
        list_counts = []
        for c_id, total_amount in data:
            list_counts.append((c_id, total_amount))
        sorted_list = sorted(list_counts, key=lambda x: x[1], reverse=True)
        top_10 = sorted_list[:10]

        yield year, top_10

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