import papaya.dist
import papaya.single
import orchpy as op
import unison

import argparse
import numpy as np

parser = argparse.ArgumentParser()
parser.add_argument('server_port', type=int, help='the port to post requests to')
parser.add_argument('client_port', type=int, help='the port to listen at')
parser.add_argument('subscriber_port', type=int, help='the port used to set up the connections')

if __name__ == "__main__":
  args = parser.parse_args()
  op.context.connect("127.0.0.1", args.server_port, args.subscriber_port, "127.0.0.1", args.client_port)
  op.register_current()
  op.register_distributed(papaya.dist)
  op.register_distributed(papaya.single)
  op.context.main_loop()

  from cprotobuf import ProtoEntity, Field

  class Test(ProtoEntity):
    objrefs = Field('bytes', 1, required=False)
