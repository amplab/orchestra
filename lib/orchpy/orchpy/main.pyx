# cython: language_level=3
#cython.wraparound=False
#cython.boundscheck=False
cimport cython
from cpython cimport array
from libc.stdint cimport uint16_t
import array
import cprotobuf
import numpy as np
import orchpy.unison as unison
import orchpy.protos_pb as pb
import types

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

cdef inline bytes get_elements(bytearray buf, int start, int len):
  cdef char *buff = PyByteArray_AS_STRING(buf)
  return PyBytes_FromStringAndSize(buff + start, len)

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
      unison.serialize(buf, arg)
      data.append(get_elements(buf, prev_index, len(buf) - prev_index))
      prev_index = len(buf)
      objrefs.append(-len(data))
  result.objrefs = objrefs
  result.data = data
  return result

cpdef deserialize_args(args, types):
  result = []
  for k in range(len(args.objrefs)):
    elem = args.objrefs[k]
    if elem >= 0: # then elem is an ObjRef
      result.append(ObjRef(elem))
    else: # then args[k] is being passed by value
      if k < len(types) - 1:
        arg_type = types[k]
      elif k == len(types) - 1 and types[-1] is not None:
        arg_type = types[k]
      elif k == len(types) - 1 and types[-1] is None:
        arg_type = types[-2]
      else:
        raise Exception()
      result.append(unison.deserialize(args.data[-elem - 1], arg_type))
  return result

cdef struct Slice:
  size_t size
  char* ptr

cdef extern void* orchestra_create_context(const char* server_addr, uint16_t reply_port, uint16_t publish_port, const char* client_addr, uint16_t client_port)
cdef extern size_t orchestra_register_function(void* context, const char* name)
cdef extern size_t orchestra_step(void* context)
cdef extern Slice orchestra_get_args(void* context)
cdef extern size_t orchestra_function_index(void* context)
cdef extern size_t orchestra_call(void* context, const char* name, const char* args, size_t argslen)
cdef extern void orchestra_map(void* context, char* name, char* args, size_t argslen, size_t* retlist)
cdef extern void orchestra_store_result(void* context, size_t objref, char* data, size_t datalen)
cdef extern size_t orchestra_get_obj_len(void* Context, size_t objref)
cdef extern char* orchestra_get_obj_ptr(void* context, size_t objref)
cdef extern size_t orchestra_pull(void* context, size_t objref)
cdef extern size_t orchestra_push(void* context)
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

  def connect(self, server_addr, reply_port, publish_port, client_addr, client_port):
    self.context = orchestra_create_context(server_addr, reply_port, publish_port, client_addr, client_port)

  def close(self):
    orchestra_destroy_context(self.context)

  def debug_info(self):
    orchestra_debug_info(self.context)

  cpdef get_object(self, ObjRef objref, type):
    index = objref.get_id()
    ptr = orchestra_get_obj_ptr(self.context, index)
    len = orchestra_get_obj_len(self.context, index)
    data = PyBytes_FromStringAndSize(ptr, len)
    return unison.deserialize(data, type)

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
  def call(self, name, arglist):
    args = serialize_args(arglist).SerializeToString()
    return ObjRef(orchestra_call(self.context, name, args, len(args)))

  def map(self, func, arglist):
    args = serialize_args(arglist).SerializeToString()
    cdef array.array result = array.array('L', len(arglist) * [0]) # TODO(pcmoritz) This might be slow
    orchestra_map(self.context, func.name, args, len(args), <size_t*>result.data.as_voidptr)
    retlist = []
    for elem in result:
      retlist.append(ObjRef(elem))
    return retlist

  """Register a function that can be called remotely."""
  def register(self, name, function, *args):
    fnid = orchestra_register_function(self.context, name)
    assert(fnid == len(self.functions))
    self.functions.append(function)
    self.arg_types.append(args)

  def pull(self, type, objref):
    objref = orchestra_pull(self.context, objref.get_id())
    return self.get_object(ObjRef(objref), type)

  def push(self, obj):
    buf = bytearray()
    unison.serialize(buf, obj)
    objref = orchestra_push(self.context)
    orchestra_store_result(self.context, objref, buf, len(buf))
    return objref

  # This will eventually be moved into the tensor library
  def assemble(self, objref):
        """Assemble an array on this node from a distributed array object reference."""
        dist_array = self.pull(np.ndarray, objref)
        return np.vstack([np.hstack([self.pull(np.ndarray, ObjRef(objref)) for objref in row]) for row in dist_array])

context = Context()

def distributed(types, return_type):
    def distributed_decorator(func):
        # deserialize arguments, execute function and serialize result
        def func_executor(args):
            arguments = []
            protoargs = deserialize_args(args, types)
            for (i, proto) in enumerate(protoargs):
              if type(proto) == ObjRef:
                if i < len(types) - 1:
                  arguments.append(context.get_object(proto, types[i]))
                elif i == len(types) - 1 and types[-1] is not None:
                  arguments.append(context.get_object(proto, types[i]))
                elif types[-1] is None:
                  arguments.append(context.get_object(proto, types[-2]))
                else:
                  raise Exception("Passed in " + str(len(args)) + " arguments to function " + func.__name__ + ", which takes only " + str(len(types)) + " arguments.")
              else:
                arguments.append(proto)
            buf = bytearray()
            unison.serialize(buf, func(*arguments))
            return memoryview(buf).tobytes()
        # for remotely executing the function
        def func_call(*args):
            return context.call(func_call.name, args)
        func_call.name = func.__name__.encode()
        func_call.is_distributed = True
        func_call.executor = func_executor
        func_call.types = types
        return func_call
    return distributed_decorator

def register_current(globallist):
  for (name, val) in globallist:
    try:
      if val.is_distributed:
        context.register(name.encode(), val.executor, *val.types)
    except AttributeError:
      pass

def register_distributed(module):
    moduledir = dir(module)
    for name in moduledir:
        val = getattr(module, name)
        try:
            if val.is_distributed:
                context.register(name.encode(), val.executor, *val.types)
        except AttributeError:
            pass
