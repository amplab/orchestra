import numpy as np
import papaya.single as single
import orchpy as op
import unison
from cprotobuf import ProtoEntity, Field

class DistArrayProto(ProtoEntity):
    shape = Field('uint64', 1, repeated=True)
    block_size = Field('uint64', 2, repeated=True)
    dtype = Field('string', 3, required=True)
    objrefs = Field(op.ObjRefsProto, 4, required=False)

class DistArray(object):
    def construct(self):
        self.dtype = self.proto.dtype
        self.shape = self.proto.shape
        self.block_size = self.proto.block_size
        self.num_blocks = [int(np.ceil(1.0 * a / b)) for (a, b) in zip(self.proto.shape, self.proto.block_size)]
        self.blocks = op.ObjRefs()
        self.blocks.from_proto(self.proto.objrefs)

    def deserialize(self, data):
        self.proto.ParseFromString(data)
        self.construct()

    def from_proto(self, data):
        self.proto = proto
        construct()

    def __init__(self, dtype='float', shape=None, block_size=None):
        self.proto = DistArrayProto()
        if shape != None:
            self.proto.shape = shape
            self.proto.block_size = block_size
            self.proto.dtype = dtype
            self.num_blocks = [int(np.ceil(1.0 * a / b)) for (a, b) in zip(self.proto.shape, self.proto.block_size)]
            objrefs = op.ObjRefs(self.num_blocks)
            self.proto.objrefs = objrefs.proto
            self.construct()

    def compute_block_lower(self, index):
        lower = []
        for i in range(len(self.block_size)):
            lower.append(index[i] * self.block_size[i])
        return lower

    def compute_block_upper(self, index):
        upper = []
        for i in range(len(self.block_size)):
            upper.append(min((index[i] + 1) * self.block_size[i], self.shape[i]))
        return upper

    def compute_block_size(self, index):
        lower = self.compute_block_lower(index)
        upper = self.compute_block_upper(index)
        return [u - l for (l, u) in zip(lower, upper)]

    def assemble(self):
        """Assemble an array on this node from a distributed array object reference."""
        result = np.zeros(self.shape)
        for index in np.ndindex(*self.num_blocks):
            lower = self.compute_block_lower(index)
            upper = self.compute_block_upper(index)
            result[[slice(l, u) for (l, u) in zip(lower, upper)]] = op.context.pull(np.ndarray, self.blocks[index])
        return result

#@op.distributed([unison.List[int], unison.List[int], str], DistArray)
def dist_zeros(shape, block_size, dtype):
    dist_array = DistArray(dtype, shape, block_size)
    for index in np.ndindex(*dist_array.num_blocks):
        dist_array.blocks[index] = single.single_zeros(dist_array.compute_block_size(index))
    return dist_array

def dist_eye(dim, block_size, dtype):
    # TODO(rkn): this code is pretty ugly, please clean it up
    assert len(block_size) == 2 # this should probably be a "raise" not an "assert"
    dist_array = dist_zeros([dim, dim], block_size, dtype)
    num_blocks = dist_array.num_blocks[0]
    for i in range(num_blocks - 1):
        dist_array.blocks[i, i] = single.single_eye(block_size[0])
    dist_array.blocks[num_blocks - 1, num_blocks - 1] = single.single_eye(dim - block_size[0] * (num_blocks - 1))
    return dist_array

#@op.distributed([unison.List[int], unison.List[int], str], DistArray)
def dist_random_normal(shape, block_size):
    dist_array = DistArray("float", shape, block_size)
    for index in np.ndindex(*dist_array.num_blocks):
        dist_array.blocks[index] = single.single_random_normal(dist_array.compute_block_size(index))
    return dist_array

@op.distributed([np.ndarray, None], np.ndarray)
def blockwise_inner(*matrices):
    n = len(matrices)
    assert(np.mod(n, 2) == 0)
    shape = (matrices[0].shape[0], matrices[n / 2].shape[1])
    result = np.zeros(shape)
    for i in range(n / 2):
        result += np.dot(matrices[i], matrices[n / 2 + i])
    return result

#@op.distributed([DistArray, DistArray], DistArray)
def dist_dot(a, b):
    assert(a.dtype == b.dtype)
    assert(len(a.shape) == len(b.shape) == 2)
    assert(a.shape[1] == b.shape[0])
    assert(a.block_size[1] == b.block_size[0])
    dtype = a.dtype
    shape = [a.shape[0], b.shape[1]]
    block_size = [a.block_size[0], b.block_size[1]]
    res = DistArray(dtype, shape, block_size)
    for i in range(res.num_blocks[0]):
        for j in range(res.num_blocks[1]):
            args = list(a.blocks[i,:]) + list(b.blocks[:,j])
            res.blocks[i,j] = blockwise_inner(*args)
    return res

# @op.distributed([DistArray], unison.Tuple[DistArray, np.ndarray])
def dist_tsqr(a):
    """
    arguments:
        a: a distributed matrix
    Suppose that
        a.shape == (M, N)
        K == min(M, N)
    return values:
        q: DistArray, if q_full = op.context.pull(DistArray, q).assemble(), then
            q_full.shape == (M, K)
            np.allclose(np.dot(q_full.T, q_full), np.eye(K)) == True
        r: np.ndarray, if r_val = op.context.pull(np.ndarray, r), then
            r_val.shape == (K, N)
            np.allclose(r, np.triu(r)) == True
    """
    # TODO: implement tsqr in two stages, first create the tree data structure
    # where each thing is an objref of a numpy array (each Q_ij is a numpy
    # array). Then assemble the matrix essentially via a map call on each Q_i0.
    assert len(a.shape) == 2
    assert a.num_blocks[1] == 1
    num_blocks = a.num_blocks[0]
    K = int(np.ceil(np.log2(num_blocks))) + 1
    q_tree = np.zeros((num_blocks, K), dtype=op.ObjRef)
    current_rs = []
    for i in range(num_blocks):
        block = a.blocks[i, 0]
        q = single.single_qr_return_q(block)
        r = single.single_qr_return_r(block)
        q_tree[i, 0] = q
        current_rs.append(r)
        assert op.context.pull(np.ndarray, q).shape[0] == op.context.pull(np.ndarray, a.blocks[i, 0]).shape[0] # TODO(rkn): remove this code at some point
        assert op.context.pull(np.ndarray, r).shape[1] == op.context.pull(np.ndarray, a.blocks[i, 0]).shape[1] # TODO(rkn): remove this code at some point
    for j in range(1, K):
        new_rs = []
        for i in range(int(np.ceil(1.0 * len(current_rs) / 2))):
            stacked_rs = single.single_vstack(*current_rs[(2 * i):(2 * i + 2)])
            q = single.single_qr_return_q(stacked_rs)
            r = single.single_qr_return_r(stacked_rs)
            q_tree[i, j] = q
            new_rs.append(r)
        current_rs = new_rs
    assert len(current_rs) == 1, "len(current_rs) = " + str(len(current_rs))

    # handle the special case in which the whole DistArray "a" fits in one block
    # and has fewer rows than columns, this is a bit ugly so think about how to
    # remove it
    if a.shape[0] >= a.shape[1]:
        q_result = DistArray(a.dtype, a.shape, a.block_size)
    else:
        q_result = DistArray(a.dtype, [a.shape[0], a.shape[0]], a.block_size)

    # reconstruct output
    for i in range(num_blocks):
        q_block_current = q_tree[i, 0]
        ith_index = i
        for j in range(1, K):
            if np.mod(ith_index, 2) == 0:
                lower = [0, 0]
                upper = [a.shape[1], a.block_size[1]]
            else:
                lower = [a.shape[1], 0]
                upper = [2 * a.shape[1], a.block_size[1]]
            ith_index /= 2
            q_block_current = single.single_dot(q_block_current, single.single_subarray(q_tree[ith_index, j], lower, upper))
        q_result.blocks[i] = q_block_current
    r = op.context.pull(np.ndarray, current_rs[0])
    assert r.shape == (min(a.shape[0], a.shape[1]), a.shape[1])
    return q_result, r

def dist_tsqr_hr(a):
    """Algorithm 6 from http://www.eecs.berkeley.edu/Pubs/TechRpts/2013/EECS-2013-175.pdf"""
    q, r_temp = dist_tsqr(a)
    y, u, s = single.single_modified_lu(q.assemble())
    s_full = np.diag(s)
    b = q.shape[1]
    #s_full = np.zeros(q.shape)
    #for i in range(b):
    #    s_full[i, i] = s[i]
    y_top = y[:b, :b]
    t = -1 * np.dot(u, np.dot(s_full, np.linalg.inv(y_top).T))
    r = np.dot(s_full, r_temp)
    return y, t, y_top, r

def dist_array_from_blocks(blocks, block_size):
    dims = len(blocks.shape)
    num_blocks = list(blocks.shape)
    shape = []
    for i in range(len(blocks.shape)):
        index = [0] * dims
        index[i] = -1
        index = tuple(index)
        remainder = op.context.pull(np.ndarray, blocks[index]).shape[i]
        shape.append(block_size[i] * (num_blocks[i] - 1) + remainder)
    dist_array = DistArray("float", shape, block_size)
    for index in np.ndindex(*blocks.shape):
        dist_array.blocks[index] = blocks[index]
    return dist_array

def dist_qr(a):
    """Algorithm 7 from http://www.eecs.berkeley.edu/Pubs/TechRpts/2013/EECS-2013-175.pdf"""
    m, n = a.shape[0], a.shape[1]
    k = min(m, n)

    # we will store our scratch work in a_work
    a_work = DistArray(a.dtype, a.shape, a.block_size)
    for index in np.ndindex(*a.num_blocks):
        a_work.blocks[index] = a.blocks[index]

    r_res = dist_zeros([k, n], a.block_size, a.dtype)
    y_res = dist_zeros([m, k], a.block_size, a.dtype)
    Ts = []

    for i in range(min(a.num_blocks[0], a.num_blocks[1])): # this differs from the paper, which says "for i in range(a.num_blocks[1])", but that doesn't seem to make any sense when a.num_blocks[1] > a.num_blocks[0]
        b = min(a.block_size[1], a.shape[1] - a.block_size[1] * i)
        column_dist_array = DistArray(a_work.dtype, [m, b], a.block_size)
        y, t, _, R = dist_tsqr_hr(dist_array_from_blocks(a_work.blocks[i:, i:(i + 1)], a.block_size))

        # print "WWW: y.shape = " + str(y.shape)
        for j in range(i, a.num_blocks[0]):
            y_res.blocks[j, i] = op.context.push(y[((j - i) * a.block_size[0]):((j - i + 1) * a.block_size[0]), :]) # eventually this should go away
        if a.shape[0] > a.shape[1]:
            # in this case, R needs to be square
            r_res.blocks[i, i] = op.context.push(np.vstack([R, np.zeros((R.shape[1] - R.shape[0], R.shape[1]))]))
        else:
            r_res.blocks[i, i] = op.context.push(R)
        Ts.append(t)

        for c in range(i + 1, a.num_blocks[1]):
            W_rcs = []
            for r in range(i, a.num_blocks[0]):
                y_ri = y[((r - i) * a.block_size[0]):((r - i + 1) * a.block_size[0]), :]
                W_rcs.append(np.dot(y_ri.T, op.context.pull(np.ndarray, a_work.blocks[r, c]))) # eventually the pull should go away
            W_c = np.sum(W_rcs, axis=0)
            for r in range(i, a.num_blocks[0]):
                y_ri = y[((r - i) * a.block_size[0]):((r - i + 1) * a.block_size[0]), :]
                # import IPython
                # IPython.embed()
                A_rc = op.context.pull(np.ndarray, a_work.blocks[r, c]) - np.dot(y_ri, np.dot(t.T, W_c))
                a_work.blocks[r, c] = op.context.push(A_rc)
            r_res.blocks[i, c] = a_work.blocks[i, c]

    q_res = dist_eye(a.shape[0], a.block_size, "float")
    # construct q_res from Ys and Ts
    #TODO(construct q_res from Ys and Ts)
    # for i in range(a.num_blocks[1]):

    #return q_res, r_res

    return Ts, y_res, r_res
