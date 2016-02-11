import unittest
import numpy as np
import orchpy as op
import orchpy.unison as unison
import subprocess, os, socket, signal
from testprograms import zeros, testfunction, testobjrefs, arrayid
import time

import papaya.dist as dist
import papaya.single as single

def get_unused_port():
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(('localhost', 0))
  addr, port = s.getsockname()
  s.close()
  return port

numworkers = 10

class Papayatest(unittest.TestCase):

  def setUp(self):
    self.incoming_port = get_unused_port()
    print "incoming port is", self.incoming_port
    self.publish_port = get_unused_port()
    print "publish port is", self.publish_port

    self.master = subprocess.Popen(["cargo", "run", "--release", "--bin", "orchestra", "--", str(self.incoming_port), str(self.publish_port)], env=dict(os.environ, RUST_BACKTRACE="1"), preexec_fn=os.setsid)
    self.workers = map(lambda worker: subprocess.Popen(["python", "testprograms.py", str(self.incoming_port), str(get_unused_port()), str(self.publish_port)], preexec_fn=os.setsid), range(numworkers))

  def testConnect(self):
    self.client_port = get_unused_port()
    op.context.connect("127.0.0.1", self.incoming_port, self.publish_port, "127.0.0.1", self.client_port)
    op.context.debug_info()

    time.sleep(1.0) # todo(pcmoritz) fix this


  def testPapayaFunctions(self):
    def test_dist_dot(d1, d2, d3):
      print "testing dist_dot with d1 = " + str(d1) + ", d2 = " + str(d2) + ", d3 = " + str(d3)
      a = dist.dist_random_normal([d1, d2], [10, 10])
      b = dist.dist_random_normal([d2, d3], [10, 10])
      c = dist.dist_dot(a, b)
      a_val = a.assemble()
      b_val = b.assemble()
      c_val = c.assemble()
      np.testing.assert_allclose(np.dot(a_val, b_val), c_val)

    def test_dist_tsqr(d1, d2):
      print "testing dist_tsqr with d1 = " + str(d1) + ", d2 = " + str(d2)
      a = dist.dist_random_normal([d1, d2], [10, 10])
      q, r = dist.dist_tsqr(a)
      a_val = a.assemble()
      q_val = q.assemble()
      np.testing.assert_allclose(np.dot(q_val, r), a_val) # check that a = q * r
      np.testing.assert_allclose(np.dot(q_val.T, q_val), np.eye(min(d1, d2)), atol=1e-6) # check that q.T * q = I
      np.testing.assert_allclose(np.triu(r), r) # check that r is upper triangular

    def test_single_modified_lu(d1, d2):
      print "testing single_modified_lu with d1 = " + str(d1) + ", d2 = " + str(d2)
      assert d1 >= d2
      k = min(d1, d2)
      m = np.random.normal(size=(d1, d2))
      q, r = np.linalg.qr(m)
      l, u, s = single.single_modified_lu(q)
      s_mat = np.zeros((d1, d2))
      for i in range(len(s)):
          s_mat[i, i] = s[i]
      np.testing.assert_allclose(q - s_mat, np.dot(l, u)) # check that q - s = l * u
      np.testing.assert_allclose(np.triu(u), u) # check that u is upper triangular
      np.testing.assert_allclose(np.tril(l), l) # check that u is lower triangular

    def test_dist_tsqr_hr(d1, d2):
      print "testing dist_tsqr_hr with d1 = " + str(d1) + ", d2 = " + str(d2)
      a = dist.dist_random_normal([d1, d2], [10, 10])
      a_val = a.assemble()
      y, t, y_top, r = dist.dist_tsqr_hr(a)
      tall_eye = np.zeros((d1, min(d1, d2)))
      np.fill_diagonal(tall_eye, 1)
      q = tall_eye - np.dot(y, np.dot(t, y_top.T))
      np.testing.assert_allclose(np.dot(q.T, q), np.eye(min(d1, d2)), atol=1e-6) # check that q.T * q = I
      np.testing.assert_allclose(np.dot(q, r), a_val) # check that a = (I - y * t * y_top.T) * r

    for i in range(10):
      d1 = np.random.randint(1, 100)
      d2 = np.random.randint(1, 100)
      d3 = np.random.randint(1, 100)
      test_dist_dot(d1, d2, d3)

    for i in range(10):
      d1 = np.random.randint(1, 50)
      d2 = np.random.randint(1, 11)
      test_dist_tsqr(d1, d2)

    for i in range(10):
      d2 = np.random.randint(1, 100)
      d1 = np.random.randint(d2, 100)
      test_single_modified_lu(d1, d2)

    for i in range(10):
      d1 = np.random.randint(1, 100)
      d2 = np.random.randint(1, 11)
      test_dist_tsqr_hr(d1, d2)

  def tearDown(self):
    os.killpg(self.master.pid, signal.SIGTERM)
    for worker in self.workers:
      os.killpg(worker.pid, signal.SIGTERM)

    # self.context.close()


if __name__ == '__main__':
    unittest.main()
