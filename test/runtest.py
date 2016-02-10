import subprocess, os, signal, time, socket
import unittest
import orchpy as op
from random import randint
import numpy as np

def get_unused_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    addr, port = s.getsockname()
    s.close()
    return port

numworkers = 5

class OrchestraTest(unittest.TestCase):

    def setUp(self):
        incoming_port = get_unused_port()
        print "incoming port is", incoming_port
        publish_port = get_unused_port()
        print "publish port is", publish_port

        self.master = subprocess.Popen(["cargo", "run", "--bin", "orchestra", "--", str(incoming_port), str(publish_port)], env=dict(os.environ, RUST_BACKTRACE="1"), preexec_fn=os.setsid)
        self.workers = map(lambda worker: subprocess.Popen(["python", "mapreduce.py", str(incoming_port), str(get_unused_port()), str(publish_port)], preexec_fn=os.setsid), range(numworkers))
        self.workers = map(lambda worker: subprocess.Popen(["python", "matmul.py", str(incoming_port), str(get_unused_port()), str(publish_port)], preexec_fn=os.setsid), range(numworkers))
        op.context.connect("127.0.0.1", incoming_port, publish_port, "127.0.0.1", get_unused_port())

    def tearDown(self):
        os.killpg(self.master.pid, signal.SIGTERM)
        for worker in self.workers:
            os.killpg(worker.pid, signal.SIGTERM)

class CallTest(OrchestraTest):

    def testCall(self):
        time.sleep(0.5)

        import mapreduce
        M = mapreduce.zeros()
        res = op.context.pull(np.ndarray, M)
        self.assertTrue(np.linalg.norm(res) < 1e-5)

        M = mapreduce.create_dist_array()
        res = op.context.pull(np.ndarray, M)

class MapTest(OrchestraTest):

    def testMap(self):
        time.sleep(0.5)

        m = 5
        import mapreduce
        from mapreduce import plusone
        args = []
        for i in range(m):
            args.append(mapreduce.zeros())
        res = op.context.map(plusone, args)
        for i in range(m):
            mat = op.context.pull(np.ndarray, res[i])

class MatMulTest(OrchestraTest):

    def testMatMul(self):
        time.sleep(1.0)

        print "starting computation"

        import matmul
        import mapreduce

        M = mapreduce.create_dist_array()
        res = matmul.matrix_multiply(M, M)

        A = op.context.assemble(M)
        B = op.context.assemble(res)

        self.assertTrue(np.linalg.norm(A.dot(A) - B) <= 1e-4)

class CallByValueTest(OrchestraTest):

    def testCallByValue(self):
        time.sleep(0.5)

        import mapreduce
        res = mapreduce.str_identity("hello world")


if __name__ == '__main__':
    unittest.main()
