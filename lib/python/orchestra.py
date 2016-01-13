import argparse
import os
from ctypes import *
import numpy as np
import types

curr_dir = os.path.dirname(__file__)
execfile(os.path.join(curr_dir, "comm_pb2.py"))
execfile(os.path.join(curr_dir, "types_pb2.py"))

DTYPE_TO_NUMPY = {"DTYPE_INT": "int32", "DTYPE_LONG": "int64", "DTYPE_FLOAT": "float32", "DTYPE_DOUBLE": "float64"}
NUMPY_TO_DTYPE = {v: k for k, v in DTYPE_TO_NUMPY.items()}

# TODO(easy): Make this more efficient
def make_array(array):
    result = Array()
    result.shape.extend(array.shape)
    result.data += str(array[:].data)
    result.dtype = DataType.Value(NUMPY_TO_DTYPE[str(array.dtype)])
    return result

# TODO(easy): Might make this generic, for any type
def make_float(val):
    result = Float()
    result.val = val
    return result

# TODO(easy): Make this more efficient
def numpy_view(array):
    dtype_name = DataType.Name(array.dtype)
    mat = np.frombuffer(array.data, dtype=DTYPE_TO_NUMPY[dtype_name])
    mat.shape = array.shape
    return mat

def deserialize(objtype, data):
    if objtype == int:
        return Int().ParseFromString(data).value
    elif objtype == np.ndarray:
        result = Array()
        result.ParseFromString(data)
        return numpy_view(result)
    else:
        return objtype.deserialize(data)


def serialize(value):
    if type(value) == int:
        result = Int()
        result.val = value
        return result.SerializeToString()
    elif type(value) == float:
        result = Float()
        result.val = value
        return result.SerializeToString()
    elif type(value) == np.ndarray:
        return make_array(value).SerializeToString()
    else:
        return value.serialize()

class OrchestraContext:
    def __init__(self):
        self.lib = CDLL("liborchestralib.so")
        self.lib.orchestra_get_arg_len.restype = c_size_t
        self.lib.orchestra_get_arg_ptr.restype = c_void_p
        self.lib.orchestra_get_obj_len.restype = c_size_t
        self.lib.orchestra_get_obj_ptr.restype = c_void_p
        self.lib.orchestra_num_args.restype = c_size_t
        self.lib.orchestra_step.restype = c_size_t
        self.lib.orchestra_pull.restype = c_size_t
        self.functions = []
        self.arg_types = []

    def connect(self, server_addr, client_addr, subscriber_port):
        print "connect to", server_addr, client_addr
        self.context = self.lib.orchestra_create_context(server_addr, client_addr, subscriber_port)

    def close(self):
        self.lib.orchestra_destroy_context(self.context)

    """Register a function that can be called remotely."""
    def register(self, name, function, *args):
        fnid = self.lib.orchestra_register_function(self.context, name)
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
        return self.lib.orchestra_call(self.context, name, arguments, num_args)

    def map(self, func, list):
        num_args = len(list)
        arguments_type = num_args * c_uint64
        arguments = arguments_type()
        results = arguments_type()
        for (i, arg) in enumerate(list):
            arguments[i] = arg
        self.lib.orchestra_map(self.context, func.name, arguments, num_args, results)
        return results

    # TODO(pcmoritz): Store type information on the server and the client to get rid of the type parameter
    def pull(self, typ, objref):
        objref = self.lib.orchestra_pull(self.context, objref)
        p = self.lib.orchestra_get_obj_ptr(self.context, objref)
        l = self.lib.orchestra_get_obj_len(self.context, objref)
        data = string_at(p, l)
        return deserialize(typ, data)

    def debug_info(self):
        self.lib.orchestra_debug_info(self.context)

    def main_loop(self):
        while True:
            objref = self.lib.orchestra_step(self.context)
            fnidx = self.lib.orchestra_function_index(self.context)
            args = []
            for i in range(self.lib.orchestra_num_args(self.context)):
                p = self.lib.orchestra_get_arg_ptr(self.context, i)
                l = self.lib.orchestra_get_arg_len(self.context, i)
                data = string_at(p, l)
                print i
                args.append(data)
            func = self.functions[fnidx]
            result = func(*args)
            self.lib.orchestra_store_result(self.context, objref, result, len(result))

    def assemble(self, objref):
        """Assemble an array on this node from a distributed array object reference."""
        dist_array = self.pull(np.ndarray, objref)
        return np.vstack([np.hstack([self.pull(np.ndarray, objref) for objref in row]) for row in dist_array])

context = OrchestraContext()

def distributed(*types):
    def distributed_decorator(func):
        def func_executor(*protoargs):
            args = []
            for (i, proto) in enumerate(protoargs):
                args.append(deserialize(types[i], proto))
            return serialize(func(*args))
        def func_call(*args):
            num_args = len(args)
            arguments_type = num_args * c_uint64
            arguments = arguments_type()
            for (i, arg) in enumerate(args):
                arguments[i] = arg
            return context.lib.orchestra_call(context.context, func.func_name, arguments, num_args)
        func_call.name = func.func_name
        func_call.is_distributed = True
        func_call.executor = func_executor
        func_call.types = types
        return func_call
    return distributed_decorator

def register_distributed(module):
    moduledir = dir(module)
    for name in moduledir:
        val = getattr(module, name)
        if isinstance(val, types.FunctionType):
            try:
                if val.is_distributed:
                    context.register(name, val.executor, *val.types)
            except AttributeError:
                pass

def register_current(globallist):
    for (name, val) in globallist:
        if isinstance(val, types.FunctionType):
            try:
                if val.is_distributed:
                    context.register(name, val.executor, *val.types)
            except AttributeError:
                pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('server_port', type=str, help='the server port')
    parser.add_argument('client_port', type=str, help='the port to listen at')
    parser.add_argument('subscriber_port', type=int, help='the port to subscribe at')
    parser.add_argument('console', type=int, help='open a console?')
    args = parser.parse_args()

    print "args.console is ", args.console

    context.connect("tcp://127.0.0.1:" + args.server_port, "tcp://127.0.0.1:" + args.client_port, args.subscriber_port)

    if args.console == 1:
        import IPython
        IPython.embed()

    dist = context.call("create_dist_matrix")

    mul = context.call("matrix_multiply", dist, dist)

    context.assemble(dist)

    context.assemble(mul)
