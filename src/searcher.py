import logging
import numpy as np
from scipy.sparse import hstack, vstack


logger = logging.getLogger("searcher")
logger.setLevel(logging.INFO)


def pairwise_sparse_jaccard_distance(X, Y=None):
    """
    Computes the Jaccard distance between two sparse matrices or between all pairs in
    one sparse matrix.

    Args:
        X (scipy.sparse.csr_matrix): A sparse matrix.
        Y (scipy.sparse.csr_matrix, optional): A sparse matrix.

    Returns:
        numpy.ndarray: A similarity matrix.
    """

    if Y is None:
        Y = X

    assert X.shape[1] == Y.shape[1]

    X, Y = (
        X.astype(bool).astype(int),
        Y.astype(bool).astype(int),
    )

    intersect = X.dot(Y.T)

    x_sum, y_sum = X.sum(axis=1).A1, Y.sum(axis=1).A1
    xx, yy = np.meshgrid(x_sum, y_sum)
    union = (xx + yy).T - intersect

    return (1 - intersect / union).A

'''
class Searcher:
    """"""

    def __init__(self):
        self.ids = []
        self.texts = []
        self.matrix = None

    def delete(self, ids: []):
        """"""
        i_v_t = [(i, v, t) for i, v, t in zip(self.ids, self.matrix, self.texts) if i not in ids]
        if i_v_t:
            ids_, vectors_, texts_ = zip(*i_v_t)
            self.ids = list(ids_)
            self.texts = list(texts_)
            self.matrix = vstack(vectors_)
        else:
            self.ids = []
            self.texts = []
            self.matrix = None

    def add(self, ids_: [], texts_: [], vectors_: []):
        """"""
        # ids_, vectors_ = zip(*ids_vectors)
        self.ids += ids_
        self.texts += texts_
        if self.matrix is None:
            self.matrix = vstack([v.T for v in vectors_])
        else:
            new_matrix = vstack([v.T for v in vectors_])
            self.matrix = vstack([self.matrix, new_matrix])

    def search(self, vectors: [], score=0.3):
        """"""
        searched_matrix = hstack(vectors).T
        jaccard_matrix = 1 - pairwise_sparse_jaccard_distance(searched_matrix, self.matrix)
        print("jaccard_matrix:", jaccard_matrix)
        logger.info("jaccard_matrix  is {}".format(str(jaccard_matrix)))
        indexes = (jaccard_matrix > score).nonzero()
        logger.info("searching indexes hire then score are {}".format(str(indexes)))
        print("indexes:", indexes)
        results = [(i, self.ids[j], self.texts[j], jaccard_matrix[i][j]) for i, j in zip(indexes[0], indexes[1])]
        print("results:", results)
        return sorted(results, key=lambda x: x[3], reverse=True)
'''