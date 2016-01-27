import orchpy as op
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('server_port', type=str, help='the port to post requests to')
parser.add_argument('subscriber_port', type=int, help='the port used to set up the connections')


if __name__ == '__main__':
  args = parser.parse_args()
  op.context.connect("tcp://127.0.0.1:" + args.server_port, "tcp://127.0.0.1:2222", args.subscriber_port)
  import IPython
  IPython.embed()
