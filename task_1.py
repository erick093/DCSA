from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string
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

        columns = re.split(r"[\t]", line)
        if columns[1] == "short" or columns[1] == "movie":
            no_punctuation = "".join([char for char in columns[2] if char not in string.punctuation])
            word_tokens = word_tokenize(no_punctuation.lower())
            no_stop_words = [w for w in word_tokens if w not in stop_words]
            is_alpha = [w for w in no_stop_words if w.isalpha()]
            for word in is_alpha:
                yield (word, 1)

    def combiner(self, word, counts):
        yield (word, sum(counts))

    def reducer(self, word, counts):
        yield None, (sum(counts), word)

    def filter(self, _, data):
        list_counts = []
        for word, count in data:
            list_counts.append((word, count))
        sorted_list = sorted(list_counts, key=lambda x: x[0], reverse=True)
        for i in range(50):
            yield sorted_list[i]

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.filter)
        ]


if __name__ == '__main__':
    MRCommonKeyWords.run()