from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from collections import Counter
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


class MRGenreKeyWords(MRJob):

    def mapper_genre(self, _, line):
        """
        The mapper will split each tabbed word [\t] and will filter and analyze
        only the words in the third column ("primaryTitle") and last column ("genres")
        whose value in the second column is "movie".
        The mapper will yield a tuple (genre,title) for each genre found in the column ("genres")
        :param _: key value, represented as none.
        :param line: raw input line.
        :return: a key value pair: (genre, title)
        """
        columns = re.split(r"[\t]", line)  # split each line according to the use of tabs
        if columns[1] == "movie":  # only process lines that belong movies
            genres = re.split(r"[,]", columns[8])  # split each genre in the "genres" column
            title = columns[2]  # save the primaryTitle value found in the third column
            for genre in genres:
                yield (genre, title)  # yield a tuple (genre,title) for each genre

    def mapper_words(self, genre, title):
        """
        This mapper will tokenize and filter the words found in the title and return a tuple for each received genre
        and a dictionary of occurrences of words in the movie title.
        :param genre: genre as key.
        :param title: title as value.
        :return: a key value pair: (genre, counts) where key is the genre and counts is a dictionary of word
        occurrences in the title.
        """

        no_punctuation = title.translate(str.maketrans('', '', string.punctuation))  # remove punctuation
        is_alpha = no_punctuation.translate({ord(ch): None for ch in '0123456789'})  # keep only alphanumerical words
        word_tokens = word_tokenize(is_alpha.lower())  # tokenize the words on a line
        no_stop_words = [w for w in word_tokens if w not in stop_words]  # remove the stopwords on a line
        yield(genre, Counter(no_stop_words))

    def combiner_words(self, key, counts):
        """
        Combine the dictionaries(counts) that have the same key(genre) in a single mapper.
        :param key: movie genre as key.
        :param counts: dictionary containing words with the counts { word:number of occurrences,...}.
        :return: yields a tuple of a key(genre) with a dictionary of the accumulated dictionary of occurrences.
        """
        sj_pro = {}  # accumulated dictionary
        for sj in counts:
            sj_pro = Counter(sj_pro) + Counter(sj)  # accumulating all the dictionaries received in counts
        yield (key, sj_pro)

    def reducer_words(self, key, counts):
        """
        Merge all the intermediate counts dictionaries associated with the same key (genre)
        :param key: movie genre as key.
        :param counts: dictionary containing words with the counts { word:number of occurrences,...}.
        :return: a key value pair: (key,dict_top_15) where the key is a movie genre and dict_top_15.keys
        contains the keysof the 15 most frequent words in each movie genre.
        """
        sj_pro = {}  # accumulated dictionary
        for sj in counts:
            sj_pro = Counter(sj_pro) + Counter(sj)  # accumulating all the dictionaries received in counts
        top_15 = sj_pro.most_common(15)  # get the 15 words with the highest number of occurrences.
        dict_top_15 = dict(top_15)   # translate top_15 from a list to a dictionary.
        yield (key, list(dict_top_15.keys()))


    def steps(self):
        """
        Defines the steps of a job, in this case the job has 2 steps:
        1.A mapper
        2.A mapper, combiner and reducer.
        """
        return [
            MRStep(mapper=self.mapper_genre),
            MRStep(mapper=self.mapper_words,
                   combiner=self.combiner_words,
                   reducer=self.reducer_words)
        ]


if __name__ == '__main__':
    MRGenreKeyWords.run()