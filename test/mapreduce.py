import numpy as np
import orchpy as op
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('server_port', type=int, help='the port to post requests to')
parser.add_argument('client_port', type=int, help='the port to listen at')
parser.add_argument('subscriber_port', type=int, help='the port used to set up the connections')

@op.distributed([], np.ndarray)
def zeros():
    return np.zeros((100, 100))

@op.distributed([str], str)
def str_identity(string):
    return string

@op.distributed([], np.ndarray)
def create_dist_array():
    objrefs = np.empty((2, 2), dtype=np.dtype("int64"))
    for i in range(2):
        for j in range(2):
            objrefs[i,j] = zeros().get_id()
    return objrefs

@op.distributed([np.ndarray], np.ndarray)
def plusone(matrix):
    return matrix + 1.0

if __name__ == "__main__":
    args = parser.parse_args()
    op.context.connect("127.0.0.1", args.server_port, args.subscriber_port, "127.0.0.1", args.client_port)
    op.register_current()
    op.context.main_loop()
