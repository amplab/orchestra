#cython.wraparound=False
#cython.boundscheck=False
cimport cython
import numpy as np
import orchpy.protos_pb as pb

include "utils.pxi"

cdef class ObjRef:
  cdef size_t _id

  def __cinit__(self, id):
    self._id = id

  def __richcmp__(self, other, int op):
    if op == 2:
      return self.get_id() == other.get_id()
    else:
      raise NotImplementedError("operator not implemented")

  cpdef get_id(self):
    return self._id

cdef int get_id(ObjRef value):
  return value._id

cdef inline str get_elements(bytearray buf, int start, int len):
  cdef char *buff = PyByteArray_AS_STRING(buf)
  return PyString_FromStringAndSize(buff + start, len)

cpdef serialize_primitive(bytearray buf, val):
  if type(val) == int or type(val) == long:
    raw_encode_uint64(buf, <uint64_t>val)
  elif type(val) == float:
    encode_float(buf, val)
  elif type(val) == str or type(val) == unicode:
    encode_string(buf, val)

cpdef serialize_args(args):
  result = pb.Args()
  cdef bytearray buf = bytearray()
  cdef size_t prev_index = 0
  objrefs = []
  data = []
  for arg in args:
    if type(arg) == ObjRef:
      objrefs.append(get_id(arg))
    else:
      prev_index = len(buf)
      serialize_primitive(buf, arg)
      data.append(get_elements(buf, prev_index, len(buf) - prev_index))
      prev_index = len(buf)
      objrefs.append(-len(data))
  result.objrefs = objrefs
  result.data = data
  return result

cpdef object deserialize(bytes data, type t):
  cdef char *buff = <char*>data
  cdef Py_ssize_t size = len(data)
  cdef char *end = buff + size
  if t == int or t == long:
    return decode_uint64(&buff, end)
  if t == float:
    return decode_float(&buff, end)
  if t == str or t == unicode:
    return decode_string(&buff, end)

cpdef deserialize_args(args, types):
  result = []
  for k in range(len(args.objrefs)):
    elem = args.objrefs[k]
    if elem >= 0:
      result.append(ObjRef(elem))
    else:
      result.append(deserialize(args.data[-elem-1], types[k]))
  return result

cdef int numpy_dtype_to_proto(dtype):
  if dtype == np.dtype('int32'):
    return pb.INT32
  if dtype == np.dtype('int64'):
    return pb.INT64
  if dtype == np.dtype('float32'):
    return pb.FLOAT32
  if dtype == np.dtype('float64'):
    return pb.FLOAT64

cpdef array_to_proto(array):
  result = pb.Array()
  result.shape.extend(array.shape)
  result.data = np.getbuffer(array, 0, array.size * array.dtype.itemsize)
  result.dtype = numpy_dtype_to_proto(array.dtype)
  return result

cdef proto_dtype_to_numpy(dtype):
  if dtype == pb.INT32:
    return np.dtype('int32')
  if dtype == pb.INT64:
    return np.dtype('int64')
  if dtype == pb.FLOAT32:
    return np.dtype('float32')
  if dtype == pb.FLOAT64:
    return np.dtype('float64')

cpdef proto_to_array(proto):
  dtype = proto_dtype_to_numpy(proto.dtype)
  result = np.frombuffer(proto.data, dtype=dtype)
  result.shape = proto.shape
  return result

cdef struct Slice:
  size_t size
  char* ptr

cdef extern void* orchestra_create_context(const char* server_addr, const char* client_addr, long subscriber_port)
cdef extern size_t orchestra_register_function(void* context, const char* name)
cdef extern size_t orchestra_step(void* context)
cdef extern Slice orchestra_get_args(void* context)
cdef extern size_t orchestra_function_index(void* context)
cdef extern size_t orchestra_call(void* context, const char* name, const char* args, size_t argslen)
cdef extern void orchestra_store_result(void* context, size_t objref, char* data, size_t datalen)
cdef extern size_t orchestra_get_obj_len(void* Context, size_t objref)
cdef extern char* orchestra_get_obj_ptr(void* context, size_t objref)
cdef extern void orchestra_debug_info(void* context)
cdef extern void orchestra_destroy_context(void* context)

cdef class Context:
  cdef void* context
  cdef public list functions
  cdef public list arg_types

  def __cinit__(self):
    self.context = NULL
    self.functions = []
    self.arg_types = []

  def connect(self, server_addr, client_addr, subscriber_port):
    self.context = orchestra_create_context(server_addr, client_addr, subscriber_port)

  def close(self):
    orchestra_destroy_context(self.context)

  def debug_info(self):
    orchestra_debug_info(self.context)

  cpdef get_object(self, ObjRef objref, type):
    index = objref.get_id()
    ptr = orchestra_get_obj_ptr(self.context, index)
    len = orchestra_get_obj_len(self.context, index)
    data = PyBytes_FromStringAndSize(ptr, len)
    return deserialize(data, type)

  def main_loop(self):
    cdef size_t objref = 0
    while True:
      objref = orchestra_step(self.context)
      fnidx = orchestra_function_index(self.context)
      slice = orchestra_get_args(self.context)
      data = PyBytes_FromStringAndSize(slice.ptr, slice.size)
      func = self.functions[fnidx]
      args = pb.Args()
      args.ParseFromString(data)
      result = func(args)
      orchestra_store_result(self.context, objref, result, len(result))

  """Args is serialized version of the arguments."""
  def call(self, name, args):
    return ObjRef(orchestra_call(self.context, name, args, len(args)))

  """Register a function that can be called remotely."""
  def register(self, name, function, *args):
    fnid = orchestra_register_function(self.context, name)
    assert(fnid == len(self.functions))
    self.functions.append(function)
    self.arg_types.append(args)

context = Context()

def distributed(*types):
    def distributed_decorator(func):
        def func_executor(args):
            arguments = []
            protoargs = deserialize_args(args, types)
            for (i, proto) in enumerate(protoargs):
              if type(proto) == ObjRef:
                arguments.append(context.get_object(proto, types[i]))
              else:
                arguments.append(proto)
            buf = bytearray()
            serialize_primitive(buf, func(*arguments))
            return memoryview(buf).tobytes()
        def func_call(*args):
            return context.call(func.func_name, serialize_args(args).SerializeToString())
        func_call.name = func.func_name
        func_call.is_distributed = True
        func_call.executor = func_executor
        func_call.types = types
        return func_call
    return distributed_decorator
