import argparse
import os
from ctypes import *
import numpy as np
import types

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

class HermesContext:
    def __init__(self):
        self.lib = CDLL("libhermeslib.so")
        self.lib.hermes_get_arg_len.restype = c_size_t
        self.lib.hermes_get_arg_ptr.restype = c_void_p
        self.lib.hermes_get_obj_len.restype = c_size_t
        self.lib.hermes_get_obj_ptr.restype = c_void_p
        self.lib.hermes_num_args.restype = c_size_t
        self.lib.hermes_step.restype = c_size_t
        self.lib.hermes_pull.restype = c_size_t
        self.functions = []
        self.arg_types = []

    def connect(self, server_addr, client_addr):
        self.context = self.lib.hermes_create_context(server_addr, client_addr)

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

    def map(self, name, list):
        num_args = len(list)
        arguments_type = num_args * c_uint64
        arguments = arguments_type()
        for (i, arg) in enumerate(list):
            arguments[i] = arg
        self.lib.hermes_map(self.context, name, arguments, num_args)

    # TODO(pcmoritz): Store type information on the server and the client to get rid of the type parameter
    def pull(self, typ, objref):
        print "before pull"
        objref = self.lib.hermes_pull(self.context, objref)
        print "after pull"
        p = self.lib.hermes_get_obj_ptr(self.context, objref)
        l = self.lib.hermes_get_obj_len(self.context, objref)
        data = string_at(p, l)
        t = typ()
        t.ParseFromString(data)
        return python_object(t)

    def debug_info(self):
        self.lib.hermes_debug_info(self.context)

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
                print i
                protodata = arg_types[i]()
                protodata.ParseFromString(data)
                args.append(protodata)
            func = self.functions[fnidx]
            result = func(*args).SerializeToString()
            self.lib.hermes_store_result(self.context, objref, result, len(result))

    def assemble(self, objref):
        """Assemble a tensor on this node from a distributed tensor object reference."""
        dist_array = self.pull(Tensor, objref)
        return np.vstack([np.hstack([self.pull(Tensor, objref) for objref in row]) for row in dist_array])

context = HermesContext()

def distributed(*types):
    def distributed_decorator(func):
        def func_executor(*protoargs):
            args = map(python_object, protoargs)
            return proto_buf(func(*args))
        def func_call(*args):
            num_args = len(args)
            arguments_type = num_args * c_uint64
            arguments = arguments_type()
            for (i, arg) in enumerate(args):
                arguments[i] = arg
            return context.lib.hermes_call(context.context, func.func_name, arguments, num_args)
        func_call.is_distributed = True
        func_call.executor = func_executor
        func_call.types = types
        return func_call
    return distributed_decorator

def register_distributed(items):
    for name, val in items:
        if isinstance(val, types.FunctionType):
            try:
                if val.is_distributed:
                    context.register(name, val.executor, *val.types)
            except AttributeError:
                pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('port', type=str, help='the port to listen at')
    parser.add_argument('console', type=int, help='open a console?')
    args = parser.parse_args()

    print "args.console is ", args.console

    context.connect("tcp://127.0.0.1:1234", "tcp://127.0.0.1:" + args.port)

    if args.console == 1:
        import IPython
        IPython.embed()

    dist = context.call("create_dist_matrix")

    mul = context.call("matrix_multiply", dist, dist)

    context.assemble(dist)

    context.assemble(mul)
