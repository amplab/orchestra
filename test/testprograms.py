import numpy as np
import orchpy as op
import orchpy.unison as unison
import argparse

import papaya.dist
import papaya.single

parser = argparse.ArgumentParser()
parser.add_argument('server_port', type=int, help='the port to post requests to')
parser.add_argument('client_port', type=int, help='the port to listen at')
parser.add_argument('subscriber_port', type=int, help='the port used to set up the connections')

@op.distributed([unicode, op.ObjRef, int], [unicode])
def testfunction(a, b, c):
  return a

@op.distributed([], [op.ObjRefs])
def testobjrefs():
  return op.ObjRefs((10,10))

@op.distributed([unison.List[int]], [np.ndarray])
def zeros(shape):
  return np.zeros(shape)

@op.distributed([np.ndarray], [np.ndarray])
def arrayid(array):
  return array

if __name__ == "__main__":
  args = parser.parse_args()
  op.context.connect("127.0.0.1", args.server_port, args.subscriber_port, "127.0.0.1", args.client_port)
  op.register_current()
  # op.register_distributed(papaya.dist)
  # op.register_distributed(papaya.single)
  op.context.main_loop()
