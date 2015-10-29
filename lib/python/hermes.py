import argparse
import os
from ctypes import *
import numpy as np

curr_dir = os.path.dirname(__file__)
execfile(os.path.join(curr_dir, "comm_pb2.py"))
execfile(os.path.join(curr_dir, "types_pb2.py"))
execfile(os.path.join(curr_dir, "tensor_pb2.py"))

DTYPE_TO_NUMPY = {"DTYPE_INT": "int32", "DTYPE_LONG": "int64", "DTYPE_FLOAT": "float32", "DTYPE_DOUBLE": "float64"}
NUMPY_TO_DTYPE = {v: k for k, v in DTYPE_TO_NUMPY.items()}

# TODO(easy): Make this more efficient
def make_tensor(array):
    tensor = Tensor()
    tensor.dims.extend(array.shape)
    tensor.data += str(array[:].data)
    tensor.dtype = DataType.Value(NUMPY_TO_DTYPE[str(array.dtype)])
    return tensor

# TODO(easy): Might make this generic, for any type
def make_float(val):
    result = Float()
    result.val = val
    return result

# TODO(easy): Make this more efficient
def numpy_view(tensor):
    dtype_name = DataType.Name(tensor.dtype)
    mat = np.frombuffer(tensor.data, dtype=DTYPE_TO_NUMPY[dtype_name])
    mat.shape = tensor.dims
    return mat

def python_object(value):
    if type(value) == Int:
        return value.val
    elif type(value) == Tensor:
        return numpy_view(value)
    else:
        print "type not known", type(value)
        print "value", value
        error

def proto_buf(value):
    if type(value) == int:
        result = Int()
        result.val = value
        return result
    elif type(value) == float:
        result = Float()
        result.val = value
        return result
    elif type(value) == np.ndarray:
        return make_tensor(value)


def distributed():
    def distributed_decorator(func):
        def func_wrapper(*protoargs):
            args = map(python_object, protoargs)
            return proto_buf(func(*args))
        return func_wrapper
    return distributed_decorator

class HermesContext:
    def __init__(self, server_addr, client_addr):
        self.lib = CDLL("libhermes.so")
        self.context = self.lib.hermes_create_context(server_addr, client_addr)
        self.functions = []
        self.arg_types = []
        # self.lib.hermes_register_type(self.context, "Tensor")
        # self.lib.hermes_register_type(self.context, "Int")
        # self.lib.hermes_register_type(self.context, "Float")

    """Register a function that can be called remotely."""
    def register(self, name, function, *args):
        fnid = self.lib.hermes_register_function(self.context, name)
        assert(fnid == len(self.functions))
        self.functions.append(function)
        self.arg_types.append(args)


    """Call a function on a remote machine, args is a list of object references"""
    def call(self, name, *args):
        num_args = len(args)
        arguments_type = num_args * c_uint64
        arguments = arguments_type()
        for (i, arg) in enumerate(args):
            arguments[i] = arg
        return self.lib.hermes_call(self.context, name, arguments, num_args)

    def main_loop(self):
        while True:
            objref = self.lib.hermes_step(self.context)
            fnidx = self.lib.hermes_function_index(self.context)
            arg_types = self.arg_types[fnidx]
            args = []
            for i in range(self.lib.hermes_num_args(self.context)):
                p = self.lib.hermes_get_arg_ptr(self.context, i)
                l = self.lib.hermes_get_arg_len(self.context, i)
                data = string_at(p, l)
                protodata = arg_types[i]()
                protodata.ParseFromString(data)
                args.append(protodata)
            func = self.functions[fnidx]
            result = func(*args).SerializeToString()
            self.lib.hermes_store_result(self.context, objref, result, len(result))


if __name__ == "__main__":
    context = HermesContext("tcp://127.0.0.1:1234", "")

    dist = context.call("create_dist_matrix")
    mul = context.call("matrix_multiply", dist, dist)

    import IPython
    IPython.embed()
