import numpy as np
import orchpy as op
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('server_port', type=str, help='the port to post requests to')
parser.add_argument('client_port', type=str, help='the port to listen at')
parser.add_argument('subscriber_port', type=int, help='the port used to set up the connections')

@op.distributed(np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray)
def blockwise_dot(*matrices):
    result = np.zeros((100, 100))
    k = len(matrices) / 2
    for i in range(k):
        result += np.dot(matrices[i], matrices[k+i])
    return result

@op.distributed(np.ndarray, np.ndarray, np.ndarray)
def matrix_multiply(first_dist_mat, second_dist_mat):
    objrefs = np.zeros((2, 2), dtype=np.dtype("int64"))
    for i in range(2):
        for j in range(2):
            args = list(map(op.ObjRef, first_dist_mat[i,:])) + list(map(op.ObjRef, second_dist_mat[:,j]))
            objrefs[i,j] = blockwise_dot(*args).get_id()
    return objrefs

if __name__ == "__main__":
    args = parser.parse_args()
    op.context.connect("tcp://127.0.0.1:" + args.server_port, "tcp://127.0.0.1:" + args.client_port, args.subscriber_port)
    op.register_current(globals().items())
    op.context.main_loop()
