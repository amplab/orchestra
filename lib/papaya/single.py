import numpy as np
import orchpy as op
import unison

@op.distributed([unison.List[int]], np.ndarray)
def single_zeros(shape):
    return np.zeros(shape)

@op.distributed([int], np.ndarray)
def single_eye(dim):
    return np.eye(dim)

@op.distributed([unison.List[int]], np.ndarray)
def single_random_normal(shape):
    return np.random.normal(size=shape)

@op.distributed([np.ndarray, np.ndarray], np.ndarray)
def single_dot(a, b):
    return np.dot(a, b)

# TODO(rkn): this should take the same optional "mode" argument as np.linalg.qr, except that the different options sometimes have different numbers of return values, which could be a problem
@op.distributed([np.ndarray], unison.Tuple[np.ndarray, np.ndarray])
def single_qr(a):
    """
    Suppose (n, m) = a.shape
    If n >= m:
        q.shape == (n, m)
        r.shape == (m, m)
    If n < m:
        q.shape == (n, n)
        r.shape == (n, m)
    """
    return np.linalg.qr(a)

# TODO(rkn): stopgap until we support returning tuples of object references
@op.distributed([np.ndarray], np.ndarray)
def single_qr_return_q(a):
    q, r = np.linalg.qr(a)
    return q

# TODO(rkn): stopgap until we support returning tuples of object references
@op.distributed([np.ndarray], np.ndarray)
def single_qr_return_r(a):
    q, r = np.linalg.qr(a)
    return r

# TODO(rkn): stopgap
@op.distributed([np.ndarray], np.ndarray)
def single_qr_return_h(a):
    h, tau = np.linalg.qr(a, mode='raw')
    return h

# TODO(rkn): stopgap
@op.distributed([np.ndarray], np.ndarray)
def single_qr_return_tau(a):
    h, tau = np.linalg.qr(a, mode='raw')
    return tau

# TODO(rkn): My preferred signature would have been
# @op.distributed([unison.List[np.ndarray]], np.ndarray) but that currently
# doesn't work because that would expect a list of ndarrays not a list of
# ObjRefs
@op.distributed([np.ndarray, None], np.ndarray)
def single_vstack(*xs):
    return np.vstack(xs)

# would have preferred @op.distributed([unison.List[np.ndarray]], np.ndarray)
@op.distributed([np.ndarray, None], np.ndarray)
def single_hstack(*xs):
    return np.hstack(xs)

# TODO(rkn): this doesn't parallel the numpy API, but we can't really slice an ObjRef, think about this
@op.distributed([np.ndarray, unison.List[int], unison.List[int]], np.ndarray)
def single_subarray(a, lower_indices, upper_indices): # TODO(rkn): be consistent about using "index" versus "indices"
    return a[[slice(l, u) for (l, u) in zip(lower_indices, upper_indices)]]

@op.distributed([np.ndarray], np.ndarray)
def single_copy(a):
    return np.copy(a)

# TODO(rkn): probably make this distributed
#@op.distributed([np.ndarray], unison.Tuple[np.ndarray, np.ndarray, np.ndarray])
def single_modified_lu(q):
    """
    takes a matrix q with orthonormal columns, returns l, u, s such that q - s = l * u
    arguments:
        q: a two dimensional orthonormal q
    return values:
        l: lower triangular
        u: upper triangular
        s: a diagonal matrix represented by its diagonal
    """
    m, b = q.shape[0], q.shape[1]
    S = np.zeros(b)

    q_work = np.copy(q)

    for i in range(b):
        S[i] = -1 * np.sign(q_work[i, i])
        q_work[i, i] -= S[i]

        # scale ith column of L by diagonal element
        q_work[(i + 1):m, i] /= q_work[i, i]

        # perform Schur complement update
        q_work[(i + 1):m, (i + 1):b] -= np.outer(q_work[(i + 1):m, i], q_work[i, (i + 1):b])

    L = np.tril(q_work)
    for i in range(b):
        L[i, i] = 1
    U = np.triu(q_work)[:b, :]
    return L, U, S
