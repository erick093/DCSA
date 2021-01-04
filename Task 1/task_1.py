from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string
# List of languages supported to remove the stopwords
lang = ['hungarian',
         'swedish',
         'kazakh',
         'norwegian',
         'finnish',
         'arabic',
         'indonesian',
         'portuguese',
         'turkish',
         'azerbaijani',
         'slovene',
         'spanish',
         'danish',
         'nepali',
         'romanian',
         'greek',
         'dutch',
         'README',
         'tajik',
         'german',
         'english',
         'russian',
         'french',
         'italian']
stop_words = set(stopwords.words(lang))


class MRCommonKeyWords(MRJob):

    def mapper(self, _, line):
        """
        The mapper will split each tabbed word [\t] and will filter and analyze
        only the words in the third column ("primaryTitle") whose values in the second column is "movies" or "shorts".
        The mapper will return all the non-stopwords as a key value pair (word,1).

        :param _: key value, represented as none.
        :param line: raw input line.
        :return: a key value pair: (word,1)
        """
        columns = re.split(r"[\t]", line)  # split each line according to the use of tabs
        if columns[1] == "short" or columns[1] == "movie":  # only process lines that belong to shorts or movies
            no_punctuation = "".join([char for char in columns[2] if char not in string.punctuation])#remove punctuation
            word_tokens = word_tokenize(no_punctuation.lower())  # tokenize the words on a line
            no_stop_words = [w for w in word_tokens if w not in stop_words]  # remove the stopwords on a line
            is_alpha = [w for w in no_stop_words if w.isalpha()]  # keep only alphanumerical words
            for word in is_alpha:
                yield (word, 1)

    def combiner(self, word, counts):
        """
        Combine the values of a key in a single mapper, it return the sum of values for a single key.
        :param word: word as key.
        :param counts: count as value.
        :return: yields one or more tuples (key,value). word as key, and the sum of counts as value.
        """
        yield (word, sum(counts))

    def reducer(self, word, counts):
        """
        Merge all the intermediate counts(values) associated with the same word(key)
        :param word: word as key.
        :param counts: counts as value.
        :return: a None key and a tuple that contains the total quantity of a word, and the word itself.
        """
        yield None, (sum(counts), word)

    def filter(self, _, data):
        """
        Filter step that will receive  all rows as the input has the same key(None), and will produce a sorted list
        according to a key in this case the counts. The filter will return the first 50 elements in the sorted list.
        :param _: None key.
        :param data: input tuples: (counts,word)
        :return: the members with the highest counts in sorted_list
        """
        list_counts = []
        for count, word in data:
            list_counts.append((count, word))  # Create a list from the input data ( represented as JSON )
        sorted_list = sorted(list_counts, key=lambda x: x[0], reverse=True)  # sort the list according the count values
        for i in range(50):  # return the first 50 members of the list
            yield sorted_list[i]

    def steps(self):
        """
        Defines the steps of a job, in this case the job has 2 steps:
        1.A mapper, combiner and a reducer.
        2.A filter step executed in a single task.
        """
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.filter)
        ]


if __name__ == '__main__':
    MRCommonKeyWords.run()