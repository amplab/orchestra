import orchpy as op
import argparse

import numpy as np
import papaya.single as single
import papaya.dist as dist
import unison

parser = argparse.ArgumentParser()
parser.add_argument('server_address', type=str, help='public ip address of the server')
parser.add_argument('server_port', type=int, help='the port to post requests to')
parser.add_argument('publish_port', type=int, help='the port for the publish channel')
parser.add_argument('client_address', type=str, help='public ip address of this client')
parser.add_argument('shell_port', type=int, help='the port at which the client listens')


if __name__ == '__main__':
  args = parser.parse_args()
  op.context.connect(args.server_address, args.server_port, args.publish_port, args.client_address, args.shell_port)
  import IPython
  IPython.embed()
