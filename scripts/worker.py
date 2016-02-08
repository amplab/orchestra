import orchpy as op
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('server_port', type=int, help='the port to post requests to')
parser.add_argument('client_port', type=int, help='the port to listen at')
parser.add_argument('subscriber_port', type=int, help='the port used to set up the connections')

@op.distributed(str, int)
def setup(filename):

  return 0

if __name__ == "__main__":
    args = parser.parse_args()
    server_ip = os.getenv('SERVER_IP', "127.0.0.1")
    op.context.connect(server_ip, args.server_port, args.subscriber_port, "127.0.0.1", args.client_port)
    op.register_current(globals().items())
    op.context.main_loop()
