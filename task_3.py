from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from collections import defaultdict
import string
class MRBestCustomers(MRJob):


    def mapper(self, _, line):

        columns = re.split(r",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", line)
        if not columns[3].isalpha():
            c_id = columns[6]
            amount = int(columns[3])*float(columns[5])
            match_year = re.search('\d{4}', columns[4])
            year = match_year.group(0)
            yield c_id, (year, amount)

    def combiner(self, c_id, amount):
        d = defaultdict(list)
        for key, val in amount:
            d[key].append(val)
        total_amount = {key: sum(values) for key, values in d.items()}

        yield c_id, list(total_amount.items()) #Convert total amount to list

    def reducer(self, c_id, amount):

        test = []
        for a in amount:
            test.append(a[0])
        d = defaultdict(list)
        for key, val in test:
            d[key].append(val)
        total_amount = {key: sum(values) for key, values in d.items()}
        tt = total_amount.items()
        for year in tt:
            yield year[0], (c_id,year[1])
        #yield c_id, list(total_amount.items())

if __name__ == '__main__':
    MRBestCustomers.run()