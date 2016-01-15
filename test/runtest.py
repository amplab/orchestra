import subprocess, os, signal, time, socket, psutil
import unittest
import orchestra
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
        incoming_port = str(get_unused_port())
        print "incoming port is", incoming_port
        publish_port = str(get_unused_port())
        print "publish port is", publish_port

        self.master = subprocess.Popen(["cargo", "run", "--bin", "orchestra", "--", incoming_port, publish_port], env=dict(os.environ, RUST_BACKTRACE="1"), preexec_fn=os.setsid)
        self.workers = map(lambda worker: subprocess.Popen(["python", "mapreduce.py", incoming_port, str(get_unused_port()), publish_port], preexec_fn=os.setsid), range(numworkers))
        self.workers = map(lambda worker: subprocess.Popen(["python", "matmul.py", incoming_port, str(get_unused_port()), publish_port], preexec_fn=os.setsid), range(numworkers))
        orchestra.context.connect("tcp://127.0.0.1:" + incoming_port, "tcp://127.0.0.1:" + str(get_unused_port()), int(publish_port))

    def tearDown(self):
        os.killpg(self.master.pid, signal.SIGTERM)
        for worker in self.workers:
            os.killpg(worker.pid, signal.SIGTERM)

class CallTest(OrchestraTest):

    def testCall(self):
        time.sleep(0.5)

        import mapreduce
        M = mapreduce.zeros()
        res = orchestra.context.pull(np.ndarray, M)
        self.assertTrue(np.linalg.norm(res) < 1e-5)

        M = mapreduce.create_dist_array()
        res = orchestra.context.pull(np.ndarray, M)

class MapTest(OrchestraTest):

    def testMap(self):
        time.sleep(0.5)

        m = 5
        import mapreduce
        from mapreduce import plusone
        args = []
        for i in range(m):
            args.append(mapreduce.zeros())
        res = orchestra.context.map(plusone, args)
        for i in range(m):
            mat = orchestra.context.pull(np.ndarray, res[i])

class MatMulTest(OrchestraTest):

    def testMatMul(self):
        time.sleep(1.0)

        print "starting computation"

        import matmul
        import mapreduce

        M = mapreduce.create_dist_array()
        res = matmul.matrix_multiply(M, M)

        A = orchestra.context.assemble(M)
        B = orchestra.context.assemble(res)

        # import IPython
        # IPython.embed()

        self.assertTrue(np.linalg.norm(A.dot(A) - B) <= 1e-4)

if __name__ == '__main__':
    unittest.main()
