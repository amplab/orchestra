import numpy as np
import orchpy as op
import unison
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('server_port', type=str, help='the port to post requests to')
parser.add_argument('client_port', type=str, help='the port to listen at')
parser.add_argument('subscriber_port', type=int, help='the port used to set up the connections')

@op.distributed(str, int, int, str)
def testfunction(a, b, c):
  return a

@op.distributed(unison.List[int], np.ndarray)
def zeros(shape):
  return np.zeros(shape)

if __name__ == "__main__":
  args = parser.parse_args()
  op.context.connect("tcp://127.0.0.1:" + args.server_port, "tcp://127.0.0.1:" + args.client_port, args.subscriber_port)
  op.register_current(globals().items())
  op.context.main_loop()
