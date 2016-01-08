import subprocess, os, signal, time, socket, psutil
import unittest
import hermes
from random import randint
import numpy as np

def get_unused_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    addr, port = s.getsockname()
    s.close()
    return port

numworkers = 5

class HermesTest(unittest.TestCase):

    def setUp(self):
        incoming_port = str(get_unused_port())
        print "incoming port is", incoming_port
        publish_port = str(get_unused_port())
        print "publish port is", publish_port

        self.master = subprocess.Popen(["cargo", "run", "--bin", "hermes", "--", incoming_port, publish_port], env=dict(os.environ, RUST_BACKTRACE="1"), preexec_fn=os.setsid)
        self.workers = map(lambda worker: subprocess.Popen(["python", "mapreduce.py", incoming_port, str(get_unused_port()), publish_port], preexec_fn=os.setsid), range(numworkers))
        self.workers = map(lambda worker: subprocess.Popen(["python", "matmul.py", incoming_port, str(get_unused_port()), publish_port], preexec_fn=os.setsid), range(numworkers))
        hermes.context.connect("tcp://127.0.0.1:" + incoming_port, "tcp://127.0.0.1:" + str(get_unused_port()), int(publish_port))

    def tearDown(self):
        os.killpg(self.master.pid, signal.SIGTERM)
        for worker in self.workers:
            os.killpg(worker.pid, signal.SIGTERM)

class CallTest(HermesTest):

    def testCall(self):
        time.sleep(0.5)

        import mapreduce
        M = mapreduce.zeros()
        res = hermes.context.pull(np.ndarray, M)
        self.assertTrue(np.linalg.norm(res) < 1e-5)

        M = mapreduce.create_dist_array()
        res = hermes.context.pull(np.ndarray, M)

class MapTest(HermesTest):

    def testMap(self):
        time.sleep(0.5)

        m = 5
        import mapreduce
        from mapreduce import plusone
        args = []
        for i in range(m):
            args.append(mapreduce.zeros())
        res = hermes.context.map(plusone, args)
        for i in range(m):
            mat = hermes.context.pull(np.ndarray, res[i])

class MatMulTest(HermesTest):

    def testMatMul(self):
        time.sleep(2.0)

        print "starting computation"

        import matmul
        import mapreduce

        M = mapreduce.create_dist_array()
        res = matmul.matrix_multiply(M, M)

        A = hermes.context.assemble(M)
        B = hermes.context.assemble(res)

        # import IPython
        # IPython.embed()

        # assertTrue(np.linalg.norm(A.dot(A) - B) <= 1e-4)

if __name__ == '__main__':
    unittest.main()
