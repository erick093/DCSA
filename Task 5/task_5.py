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
        """
        Defines the steps of a job, in this case the job has 2 steps:
        1.A mapper and a reducer.
        2.A reducer.
        """
        return [
            MRStep(mapper_raw=self.mapper_raw,
                   reducer=self.reducer),
            MRStep(reducer=self.reducerMax)
        ]

    def mapper_raw(self, input_path, input_uri):
        """
        The mapper will receive a JSON file as input, read the file and extract the summary of each object and create
        a bag of words embedding of all the summaries. This mapper will also generate a random number according to the
        number of rows in the resulting bag of words embedding matrix. The mapper will yield a tuple key-value, and the
        key consist of a tuple of a summary ID  and the generated sample ID, and the value is a tuple of a word embedding
        representation of a summary and the random summary array representation.

        :param input_path: input path of the json file
        :param input_uri: input uri
        :return: a tuple ((sum_id, randID),(arrSum, randSum))
        """
        arxiv = json.load(open(input_path))  # load and read the json input file
        summaries = []
        for sum in arxiv:
            summaries.append(sum["summary"])
        vectorizer = CountVectorizer(lowercase=True, stop_words="english")  # Initialize the Bag Of Words model
        vectorizer.fit(summaries)  # learn the vocabulary dictionary of the summaries list
        vecSum = vectorizer.transform(summaries)   # transform the summaries into summaries-term matrix (bag of words)
        arrSum = vecSum.toarray()  # converts vecSum to an array
        randIdx = np.random.choice(arrSum.shape[0], size=1)  # generate a random number generated from the number of rows in arrSum
        randID = arxiv[randIdx[0]]["id"]  # obtain the ID of randIdx
        randSum = arrSum[randIdx, :][0]  # obtain the summary bag of words representation of the sample randIdx
        for idx, sum in enumerate(arxiv):
            if sum["id"] != randID:
                yield (sum["id"], randID), (arrSum[idx].tolist(), randSum.tolist())

    def reducer(self, id, wordEmbedds):
        """
        Calculate the cosine distance between two input summaries(wordEmbedds) and return a tuple: (id, cosine distance)

        :param id:
        :param wordEmbedds:
        :return:
        """
        for sum, randSum in wordEmbedds:
            cosineDist = cosine_distance(sum, randSum)  # calculates the cosine distance between the two summaries
        yield None, (id, cosineDist)

    def reducerMax(self,_ , data):
        """
        Yields the data tuple key-value: ( id, cosineDis) with highest cosine distance value
        :param _: key as none.
        :param data:  tuple key-value: ( id, cosineDis).
        """
        yield max(data)


if __name__ == '__main__':
    MRCosineDistance.run()