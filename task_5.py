from mrjob.job import MRJob
from mrjob.step import MRStep
from sklearn.feature_extraction.text import CountVectorizer
import json
import numpy as np

def cosine_distance(sumOne, sumTwo):
    """
    Calculates the cosine similarity score between two given summaries
    :param sumOne: list containing the word embedding(vector representation) of a summary
    :param sumTwo: list containing the word embedding(vector representation) of a summary
    :return: returns cosine distance coefficient, indicating how close are the summaries.
    """
    dot_product = np.dot(sumOne, sumTwo)
    norm_sumOne = np.linalg.norm(sumOne)
    norm_sumTwo = np.linalg.norm(sumTwo)
    cos = dot_product / (norm_sumOne * norm_sumTwo)
    return cos


class MRCosineDistance(MRJob):

    def steps(self):
        return [
            MRStep(mapper_raw=self.mapper_raw,
                   reducer=self.reducer),
            MRStep(reducer=self.reducerMax)
        ]

    def mapper_raw(self, input_path, input_uri):
        """

        :param input_path:
        :param input_uri:
        :return:
        """
        arxiv = json.load(open(input_path))
        summaries = []
        for sum in arxiv:
            summaries.append(sum["summary"])
        vectorizer = CountVectorizer(lowercase=True, stop_words="english")
        vectorizer.fit(summaries)
        vecSum = vectorizer.transform(summaries)
        arrSum = vecSum.toarray()
        randIdx = np.random.choice(arrSum.shape[0], size=1)
        randID = arxiv[randIdx[0]]["id"]
        randSum = arrSum[randIdx, :][0]
        for idx, sum in enumerate(arxiv):
            if sum["id"] != randID:
                yield (sum["id"], randID), (arrSum[idx].tolist(), randSum.tolist())

    def reducer(self, id, wordEmbedds):
        """

        :param id:
        :param wordEmbedds:
        :return:
        """
        for sum, randSum in wordEmbedds:
            cosineDist = cosine_distance(sum, randSum)
        yield None, (id, cosineDist)
    def reducerMax(self,_ , data):
        """

        :param _:
        :param data:
        :return:
        """
        yield max(data)
if __name__ == '__main__':
    MRCosineDistance.run()