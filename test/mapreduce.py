import numpy as np
import hermes
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('server_port', type=str, help='the port to post requests to')
parser.add_argument('client_port', type=str, help='the port to listen at')
parser.add_argument('subscriber_port', type=int, help='the port used to set up the connections')

@hermes.distributed(np.ndarray)
def zeros():
    return np.zeros((100, 100))

@hermes.distributed(np.ndarray)
def create_dist_array():
    objrefs = np.zeros((2, 2), dtype="int64")
    for i in range(2):
        for j in range(2):
            objrefs[i,j] = zeros()
    return objrefs

@hermes.distributed(np.ndarray, np.ndarray)
def plusone(matrix):
    return matrix + 1.0

if __name__ == "__main__":
    args = parser.parse_args()
    hermes.context.connect("tcp://127.0.0.1:" + args.server_port, "tcp://127.0.0.1:" + args.client_port, args.subscriber_port)
    hermes.register_current(globals().items())
    hermes.context.main_loop()
