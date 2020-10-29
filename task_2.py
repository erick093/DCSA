from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from collections import Counter
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


class MRGenreKeyWords(MRJob):

    def mapper_genre(self, _, line):
        columns = re.split(r"[\t]", line)
        if  columns[1] == "movie":
            genres = re.split(r"[,]", columns[8])
            title = columns[2]
            for genre in genres:
                yield (genre, title)

    def mapper_words(self, genre, title):

        no_punctuation = title.translate(str.maketrans('', '', string.punctuation))
        is_alpha = no_punctuation.translate({ord(ch): None for ch in '0123456789'})
        word_tokens = word_tokenize(is_alpha.lower())
        no_stop_words = [w for w in word_tokens if w not in stop_words]
        yield(genre, Counter(no_stop_words))

    def combiner_words(self, key, counts):
        sj_pro = {}
        for sj in counts:
            sj_pro = Counter(sj_pro) + Counter(sj)
        yield (key, sj_pro)

    def reducer_words(self, key, counts):
        sj_pro = {}
        for sj in counts:
            sj_pro = Counter(sj_pro) + Counter(sj)
        top_15 = sj_pro.most_common(15)
        dict_top_15 = dict(top_15)
        yield (key, list(dict_top_15.keys()))


    def steps(self):
        return [
            MRStep(mapper=self.mapper_genre),
            MRStep(mapper=self.mapper_words,
                   combiner=self.combiner_words,
                   reducer=self.reducer_words)
        ]


if __name__ == '__main__':
    MRGenreKeyWords.run()