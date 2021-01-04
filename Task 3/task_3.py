from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import datetime


class MRBestCustomers(MRJob):

    def mapper(self, _, line):
        """
        The mapper will parse the input CSV-like rows of data, extract the customer id from the Customer ID column
        , calculate the revenue and extract the year of the InvoiceDate column and return a tuple ((c_id,year), amount)
        for each input line of data.
        :param _: key value, represented as none.
        :param line: raw input line.
        :return: a tuple ((c_id,year), amount)
        """
        columns = re.split(r",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", line)  # regex to parse CSV rows of data into a list
        if not columns[3].isalpha():  # avoid the first row of the data, check if Quantity is an alphanumeric value
            c_id = columns[6]  # save the customer id
            amount = int(columns[3])*float(columns[5])  # get the revenue = Quantity * Price
            match_year = re.search('\d{4}', columns[4])  # extract the year from the InvoiceDate column
            year = match_year.group(0)  # extract the group of the re match
            yield ((c_id, year), amount)

    def combiner(self, id_year, amount):
        """
        Combine (sum) the values of a key (id_year) in a single mapper, it return the sum of values(amount)
        for a single key.
        :param id_year: id_year as key.
        :param amount: amount as value.
        :return: a tuple key value: (id_year, amount).
        """
        yield id_year, sum(amount)

    def reducer(self, id_year, amount):
        """
        Merge all the intermediate amount values associated with the same key (id_year).
        :param id_year: id_year as key.
        :param amount: amount as value.
        :return: a tuple key value: (year, (c_id,total_amount))
        """
        c_id, year = id_year
        total_amount = sum(amount)
        yield year, (c_id, total_amount)

    def reducer_aggregate(self, year, data):
        """
        Merge all the intermediate (customer_id,total_amount) values associated with the same key (year).

        :param year: year as key.
        :param data: data (c_id, total_amount) as value.
        :return: a tuple key value: (year, top_10).
        """
        list_counts = []
        for c_id, total_amount in data:
            list_counts.append((c_id, total_amount))  # append (c_id, total_amount) tuples into list_counts list
        sorted_list = sorted(list_counts, key=lambda x: x[1], reverse=True)#sort the list according to the total_amount
        top_10 = sorted_list[:10]  # extract the first 10 values of the sorted_list

        yield year, top_10

    def steps(self):
        """
        Defines the steps of a job, in this case the job has 2 steps:
        1.A mapper, combiner and reducer.
        2.A reducer.
        """
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