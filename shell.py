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

  def test_dist_qr(d1, d2):
      print "testing dist_qr with d1 = " + str(d1) + ", d2 = " + str(d2)
      a = dist.dist_random_normal([d1, d2], [10, 10])
      a_val = a.assemble()
      Ts, y_res, r_res = dist.dist_qr(a)
      # import IPython
      # IPython.embed()

      # for j in range(y_res.num_blocks[1]):
      #    for i in range(y_res.num_blocks[0]):
      #        print "i,j = " + str((i, j)) + ", shape = " + str(op.context.pull(np.ndarray, y_res.blocks[i, j]).shape)
      # import sys
      # sys.stdout.flush()

      r_val = r_res.assemble()
      y_val = y_res.assemble()

      q = np.eye(d1)
      for i in range(min(a.num_blocks[0], a.num_blocks[1])):
          q = np.dot(q, np.eye(d1) - np.dot(y_val[:, (i * 10):((i + 1) * 10)], np.dot(Ts[i], y_val[:, (i * 10):((i + 1) * 10)].T)))

      print "q shape is " + str(q.shape)
      print "r shape is " + str(r_val.shape)

      q = q[:, :min(d1, d2)]

      np.testing.assert_allclose(np.triu(r_val), r_val) # check that r is upper triangular
      np.testing.assert_allclose(np.dot(q.T, q), np.eye(min(d1, d2)), atol=1e-6) # check that q.T * q = I
      np.testing.assert_allclose(np.dot(q, r_val), a_val) # check that a = q * r


  for i in range(1):
      d1 = np.random.randint(1, 100)
      d2 = np.random.randint(1, 100)
      d3 = np.random.randint(1, 100)
      test_dist_dot(d1, d2, d3)

  for i in range(1):
      d1 = np.random.randint(1, 50)
      d2 = np.random.randint(1, 11)
      test_dist_tsqr(d1, d2)

  for i in range(1):
      d2 = np.random.randint(1, 100)
      d1 = np.random.randint(d2, 100)
      test_single_modified_lu(d1, d2)

  for i in range(1):
      d1 = np.random.randint(1, 100)
      d2 = np.random.randint(1, 11)
      test_dist_tsqr_hr(d1, d2)

  for i in range(100):
      d1 = np.random.randint(1, 100)
      d2 = np.random.randint(1, 100)
      test_dist_qr(d1, d2)

  import IPython
  IPython.embed()
