import hermes
import numpy as np
import argparse
import types

parser = argparse.ArgumentParser()
parser.add_argument('port', type=str, help='the port to listen at')
args = parser.parse_args()

@hermes.distributed(hermes.Array)
def create_matrix():
    return np.random.rand(100, 100)

@hermes.distributed(hermes.array)
def create_dist_matrix():
    objrefs = np.zeros((2, 2), dtype="int64")
    for i in range(2):
        for j in range(2):
            objrefs[i,j] = create_matrix()
            print "create_dist_matrix", objrefs[i,j]
    return objrefs

@hermes.distributed(hermes.Array, hermes.Array, hermes.Array, hermes.Array)
def pairwise_reduce(*matrices):
    result = np.zeros((100, 100))
    k = len(matrices) / 2
    for i in range(k):
        result += np.dot(matrices[i], matrices[k+i])
    print "return from pairwise_reduce"
    return result

@hermes.distributed(hermes.Array, hermes.Array, hermes.Array)
def matrix_multiply(first_dist_mat, second_dist_mat):
    objrefs = np.zeros((2, 2), dtype="int64")
    for i in range(2):
        for j in range(2):
            args = list(first_dist_mat[i,:]) + list(second_dist_mat[:,j])
            objrefs[i,j] = pairwise_reduce(*args)
            print "matrix_multiply ", objrefs[i,j]
    return objrefs

hermes.context.connect("tcp://127.0.0.1:1234", "tcp://127.0.0.1:" + args.port)
hermes.register_distributed(globals().items())

# import IPython
# IPython.embed()

hermes.context.main_loop()
