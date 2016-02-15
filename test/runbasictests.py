import unittest
import numpy as np
import orchpy as op
import orchpy.unison as unison
import subprocess, os, socket, signal
from testprograms import zeros, testfunction, testobjrefs, arrayid
import time


def get_unused_port():
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(('localhost', 0))
  addr, port = s.getsockname()
  s.close()
  return port

class UnisonTest(unittest.TestCase):

  def testSerializeArray(self):
    buf = bytearray()
    a = np.zeros((100, 100))
    unison.serialize(buf, a)
    data = memoryview(buf).tobytes()
    res = unison.deserialize(data, np.ndarray)
    self.assertTrue(np.alltrue(a == res))

    buf = bytearray()
    t = (123, 42)
    unison.serialize(buf, t)
    schema = unison.Tuple[int, int]
    data = memoryview(buf).tobytes()
    res = unison.deserialize(data, schema)
    self.assertTrue(t == res)

    buf = bytearray()
    l = ([1, 2, 3, 4], [1.0, 2.0])
    unison.serialize(buf, l)
    schema = unison.Tuple[unison.List[int], unison.List[float]]
    data = memoryview(buf).tobytes()
    res = unison.deserialize(data, schema)
    self.assertTrue(l == res)

class UnisonTest(unittest.TestCase):
  def testTypeCheck(self):
    l = 200 * [op.ObjRef(1)] + 50 * [1L] + 50 * [1.0] + 50 * [u"hi"]
    t = 200 * [op.ObjRef] + 50 * [int] + 50 * [float] + 50 * [unicode]
    op.check_types(l, t)
    try:
      l = [1, 2, 3, 4, "hi"]
      t = unison.List[int]
      op.check_types([l], [t])
      self.assertFalse(True)
    except:
      self.assertTrue(True)
    l = ("hello", "world")
    t = unison.Tuple[str, str]
    op.check_types([l], [t])
    l = [[1, 2, 3], ([1, 2, 3], ("hello", "world")), np.array([1.0, 2.0, 3.0])]
    t = [unison.List[int], unison.Tuple[unison.List[int], unison.Tuple[str, str]], np.ndarray]
    op.check_types(l, t)

class OrchestraTest(unittest.TestCase):

  def testArgs(self):
    l = 200 * [op.ObjRef(1)] + 50 * [1L] + 50 * [1.0] + 50 * [u"hi"]
    t = 200 * [op.ObjRef] + 50 * [int] + 50 * [float] + 50 * [unicode]
    args = op.serialize_args(l)
    res = op.deserialize_args(args, t)
    self.assertTrue(res == l)

  def testDistributed(self):
    l = [u"hello", op.ObjRef(2), 3]
    args = op.serialize_args(l)
    res = op.deserialize_args(args, testfunction.types)
    self.assertTrue(res == l)

numworkers = 2

class ClientTest(unittest.TestCase):

  def setUp(self):
    self.incoming_port = get_unused_port()
    print "incoming port is", self.incoming_port
    self.publish_port = get_unused_port()
    print "publish port is", self.publish_port

    self.master = subprocess.Popen(["cargo", "run", "--release", "--bin", "orchestra", "--", str(self.incoming_port), str(self.publish_port)], env=dict(os.environ, RUST_BACKTRACE="1"), preexec_fn=os.setsid)
    self.workers = map(lambda worker: subprocess.Popen(["python", "testprograms.py", str(self.incoming_port), str(get_unused_port()), str(self.publish_port)], preexec_fn=os.setsid, env=dict(os.environ, RUST_BACKTRACE="1")), range(numworkers))

  def testConnect(self):
    self.client_port = get_unused_port()
    op.context.connect("127.0.0.1", self.incoming_port, self.publish_port, "127.0.0.1", self.client_port)
    op.context.debug_info()

    time.sleep(1.0) # todo(pcmoritz) fix this

    res = zeros([100, 100])
    objrefs = testobjrefs()

    arrayid(res)

    res = op.context.pull(op.ObjRefs, objrefs)


  def tearDown(self):
    os.killpg(self.master.pid, signal.SIGTERM)
    for worker in self.workers:
      os.killpg(worker.pid, signal.SIGTERM)

    # self.context.close()


if __name__ == '__main__':
    unittest.main()
