# Prototype of the Hermes client library in Python (will be implemented in Rust
# with interfaces to many different programming languages)

from nanomsg import Socket, REQ, REP
import os
import logging
import sys
import argparse
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
        def func_wrapper(context, *protoargs):
            args = map(python_object, protoargs)
            return proto_buf(func(*args))
        return func_wrapper
    return distributed_decorator

class Function:
    # can construct itself from a bunch of messages
    def __init__(self, function, *types):
        self.function = function
        self.arg_types = types[0:-1]
        self.args = len(self.arg_types) * [None]
        self.result_type = types[-1]

    def recv_args_and_call(self, context, socket, arglist):
        num_received = 0
        data_id = -1
        unique_args = list(set(arglist))
        while num_received < len(unique_args):
            context.logger.debug("waiting for argument")
            arg = socket.recv()
            data = ServerMessage()
            data.ParseFromString(arg)
            print data.type
            context.logger.debug("received argument {}".format(data.blob.objref))
            indices = [i for i, x in enumerate(arglist) if x == data.blob.objref]

            for data_id in indices:
                print "data_id ", data_id
                self.args[data_id] = self.arg_types[data_id]()
                self.args[data_id].ParseFromString(data.blob.data)

            num_received += 1

            ack = ClientMessage()
            ack.type = ClientMessage.TYPE_ACK
            context.socket.send(ack.SerializeToString())

        context.socket.recv() # this should be a done call

        return self.function(context, *self.args)

class HermesContext:
    def __init__(self, comm_addr, client_addr):
        initsocket = Socket(REQ)
        initsocket.connect(comm_addr)
        if client_addr != "":
            reg = ClientMessage()
            reg.type = ClientMessage.TYPE_REGISTER
            reg.address = client_addr
            initsocket.send(reg.SerializeToString())
            self.socket = Socket(REP)
            self.socket.bind(client_addr)
        else:
            self.socket = initsocket
        self.objectstore = {}
        self.functions = {}
        formatter = logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
        self.logger = logging.getLogger("")
        self.logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    """Call a function on a remote machine, args is a list of object references"""
    def call(self, function_name, *args):
        self.logger.debug("client is calling function {}".format(function_name))
        request = ClientMessage()
        request.type = ClientMessage.TYPE_INVOKE
        fncall = Call()
        fncall.name = function_name
        fncall.args.extend(args)
        request.call.CopyFrom(fncall)
        self.socket.send(request.SerializeToString())
        answer = self.socket.recv()
        servermsg = ServerMessage()
        servermsg.ParseFromString(answer)
        return servermsg.objref

    """Construct a full distributed matrix locally"""
    def assemble(self, objref):
        tensor = self.pull(objref, Tensor)
        (m, n) = tensor.dims
        view = numpy_view(tensor)
        res = []
        for i in range(m):
            l = []
            for j in range(n):
                l.append(numpy_view(self.pull(view[i,j], Tensor)))
            res.append(np.hstack(l))
        return np.vstack(res)


    """Get an object from a remote machine, will block and return the object."""
    def pull(self, objref, typ):
        request = ClientMessage()
        request.type = ClientMessage.TYPE_PULL
        request.objref = objref

        self.socket.send(request.SerializeToString())

        answer = self.socket.recv()
        pushmsg = ServerMessage()
        pushmsg.ParseFromString(answer)

        test = typ()
        test.ParseFromString(pushmsg.blob.data)
        return test

    """Register a function that can be called remotely."""
    def register(self, function_name, function):
        self.functions[function_name] = function

    def main_loop(self):
        while True:
            request = self.socket.recv()
            servermsg = ServerMessage()
            servermsg.ParseFromString(request)
            if servermsg.type == ServerMessage.TYPE_INVOKE:
                self.logger.debug("got invoke request")
                fncall = servermsg.call
                self.logger.debug("request {} received".format(fncall))
                self.fncall = fncall
                self.logger.debug("function {} called".format(fncall.name))
                ack = ClientMessage()
                ack.type = ClientMessage.TYPE_ACK
                ack.call.CopyFrom(fncall)
                self.socket.send(ack.SerializeToString())
                result = self.functions[fncall.name].recv_args_and_call(self, self.socket, fncall.args)
                self.logger.debug("function evaluation done")
                self.objectstore[fncall.result] = result
                answer = ClientMessage()
                answer.type = ClientMessage.TYPE_DONE
                answer.call.CopyFrom(fncall)
                self.socket.send(answer.SerializeToString())
            elif servermsg.type == ServerMessage.TYPE_PULL:
                self.logger.debug("got pull request")
                objref = servermsg.objref
                push = ClientMessage()
                push.type = ClientMessage.TYPE_PUSH
                blob = Blob()
                blob.objref = objref
                blob.data = self.objectstore[objref].SerializeToString()
                push.blob.CopyFrom(blob)
                self.socket.send(push.SerializeToString())
            elif servermsg.type == ServerMessage.TYPE_DONE:
                continue



if __name__ == "__main__":
    context = HermesContext("tcp://127.0.0.1:1234", "")
    answer = context.call("create_dist_matrix", 0)
    res = context.call("matrix_multiply", answer, answer)

    import IPython
    IPython.embed()

    context.assemble(res)

    context.pull(res, Tensor)
