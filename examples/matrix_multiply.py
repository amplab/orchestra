import hermes
import numpy as np
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('port', type=str, help='the port to listen at')
args = parser.parse_args()

context = hermes.HermesContext("tcp://127.0.0.1:1234", "tcp://127.0.0.1:" + args.port)

@hermes.distributed()
def create_matrix():
    return np.random.rand(3, 3)

@hermes.distributed()
def create_dist_matrix():
    objrefs = np.zeros((2, 2), dtype="int64")
    for i in range(2):
        for j in range(2):
            objrefs[i,j] = context.call("create_matrix")
    return objrefs

@hermes.distributed()
def pairwise_reduce(*matrices):
    result = np.zeros((3, 3))
    k = len(matrices) / 2
    for i in range(k):
        result += np.dot(matrices[i], matrices[k+i])
    return result

@hermes.distributed()
def matrix_multiply(first_dist_mat, second_dist_mat):
    objrefs = np.zeros((2, 2), dtype="int64")
    for i in range(2):
        for j in range(2):
            args = list(first_dist_mat[i,:]) + list(second_dist_mat[:,j])
            objrefs[i,j] = context.call("pairwise_reduce", *args)
            print "matrix_multiply ", objrefs[i,j]
    return objrefs

context.register("create_matrix", create_matrix, hermes.Tensor)
context.register("create_dist_matrix", create_dist_matrix, hermes.Tensor)
context.register("pairwise_reduce", pairwise_reduce, hermes.Tensor, hermes.Tensor, hermes.Tensor, hermes.Tensor, hermes.Tensor)
context.register("matrix_multiply", matrix_multiply, hermes.Tensor, hermes.Tensor, hermes.Tensor)

context.main_loop()
