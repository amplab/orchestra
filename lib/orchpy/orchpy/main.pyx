from __future__ import unicode_literals

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

# see http://python-future.org/stdlib_incompatibilities.html
from future.utils import bytes_to_native_str

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

# this is a draft of the implementation, eventually we will use Python 3's typing
# module and this backport:
# https://github.com/python/typing/blob/master/python2/typing.py

cpdef check_type(val, t):
  if type(val) == ObjRef:
    # at the moment, obj references can be of any type; think about making them typed
    return
  if type(val) == list:
    for i, elem in enumerate(val):
      try:
        check_type(elem, t[1])
      except:
        raise Exception("Type error: Heterogeneous list " + str(val) + " at index " + str(i))
    return
  if type(val) == tuple:
    for i, elem in enumerate(val):
      try:
        check_type(elem, t[1][i])
      except:
        raise Exception("Type error: Type " + str(val) + " at index " + str(i) + " does not match")
    return
  if (type(val) == int or type(val) == long) and (t == int or t == long):
    return True
  if type(val) != t:
    raise Exception("Type of " + str(val) + " is not " + str(t))

# eventually move this into unison
cpdef check_types(vals, schema):
  for i, val in enumerate(vals):
    check_type(val, schema[i])

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

cdef struct ObjRefs:
  size_t size
  uint64_t* ptr

cdef extern void* orchestra_create_context(const char* server_addr, uint16_t reply_port, uint16_t publish_port, const char* client_addr, uint16_t client_port)
cdef extern size_t orchestra_register_function(void* context, const char* name, uint64_t num_return_vals)
cdef extern ObjRefs orchestra_step(void* context)
cdef extern Slice orchestra_get_args(void* context)
cdef extern size_t orchestra_function_index(void* context)
cdef extern ObjRefs orchestra_call(void* context, const char* name, const char* args, size_t argslen)
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
  cdef public list num_return_vals

  def __cinit__(self):
    self.context = NULL
    self.functions = []

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
    # cdef size_t objref = 0
    while True:
      objrefs = orchestra_step(self.context)
      fnidx = orchestra_function_index(self.context)
      slice = orchestra_get_args(self.context)
      data = PyBytes_FromStringAndSize(slice.ptr, slice.size)
      func = self.functions[fnidx]
      args = pb.Args()
      args.ParseFromString(data)
      result = func(args)
      for i in range(objrefs.size):
        print(objrefs.ptr[i])
        orchestra_store_result(self.context, objrefs.ptr[i], result[i], len(result[i]))

  """Args is serialized version of the arguments."""
  def call(self, func_name, module_name, arglist, num_return_vals):
    args = serialize_args(arglist).SerializeToString()
    objrefs = orchestra_call(self.context, module_name + "." + func_name, args, len(args))
    retlist = []
    for i in range(objrefs.size):
      retlist.append(ObjRef(objrefs.ptr[i]))
    return tuple(retlist)

  def map(self, func, arglist):
    arraytype = bytes_to_native_str(b'L')
    args = serialize_args(arglist).SerializeToString()
    cdef array.array result = array.array(arraytype, len(arglist) * [0]) # TODO(pcmoritz) This might be slow
    orchestra_map(self.context, func.name, args, len(args), <size_t*>result.data.as_voidptr)
    retlist = []
    for elem in result:
      retlist.append(ObjRef(elem))
    return retlist

  """Register a function that can be called remotely."""
  def register(self, func_name, module_name, function, arg_types, return_types):
    fnid = orchestra_register_function(self.context, module_name + "." + func_name, len(return_types))
    assert(fnid == len(self.functions))
    self.functions.append(function)
    self.num_return_vals.append(len(return_types))

  def pull(self, type, objref):
    objref = orchestra_pull(self.context, objref.get_id())
    return self.get_object(ObjRef(objref), type)

  def push(self, obj):
    buf = bytearray()
    unison.serialize(buf, obj)
    objref = orchestra_push(self.context)
    orchestra_store_result(self.context, objref, buf, len(buf))
    return ObjRef(objref)

context = Context()

def distributed(arg_types, return_types):
    def distributed_decorator(func):
        # deserialize arguments, execute function and serialize result
        def func_executor(args):
            arguments = []
            protoargs = deserialize_args(args, arg_types)
            for (i, proto) in enumerate(protoargs):
              if type(proto) == ObjRef:
                if i < len(arg_types) - 1:
                  arguments.append(context.get_object(proto, arg_types[i]))
                elif i == len(arg_types) - 1 and arg_types[-1] is not None:
                  arguments.append(context.get_object(proto, arg_types[i]))
                elif arg_types[-1] is None:
                  arguments.append(context.get_object(proto, arg_types[-2]))
                else:
                  raise Exception("Passed in " + str(len(args)) + " arguments to function " + func.__name__ + ", which takes only " + str(len(arg_types)) + " arguments.")
              else:
                arguments.append(proto)
            return_buffers = []
            result = func(*arguments)

            # check return type
            if len(return_types) == 1:
              if unison.unison_type(result) != return_types[0]:
                raise Exception("Return type of " + func.func_name + " does not match the return type specified in the @distributed decorator, was expecting " + str(return_types[0]) + " but received " + str(unison.unison_type(result)))
              buf = bytearray()
              unison.serialize(buf, result)
              return_buffers.append(memoryview(buf).tobytes())
            else:
              for i in range(len(return_types)):
                if unison.unison_type(result[i]) != return_types[i]:
                  raise Exception("Return type of argument " + str(i) + " for function " + func.func_name + " does not match the return type specified in the @distributed decorator, was expecting " + str(return_types[i]) + " but received " + str(unison.unison_type(result[i])))
                buf = bytearray()
                unison.serialize(buf, result[i])
                return_buffers.append(memoryview(buf).tobytes())

            return return_buffers
        # for remotely executing the function
        def func_call(*args, typecheck=False):
          if typecheck:
            check_types(args, func_call.arg_types)
          return context.call(func_call.func_name, func_call.module_name, args, len(return_types))
        func_call.func_name = func.__name__.encode() # why do we call encode()?
        func_call.module_name = func.__module__.encode() # why do we call encode()?
        func_call.is_distributed = True
        func_call.executor = func_executor
        func_call.arg_types = arg_types
        func_call.return_types = return_types
        return func_call
    return distributed_decorator

def register_current():
  for (name, val) in globals().items():
    try:
      if val.is_distributed:
        context.register(name.encode(), __name__, val.executor, val.arg_types, val.return_types)
    except AttributeError:
      pass

def register_distributed(module):
    moduledir = dir(module)
    for name in moduledir:
        val = getattr(module, name)
        try:
            if val.is_distributed:
                context.register(name.encode(), module.__name__, val.executor, val.arg_types, val.return_types)
        except AttributeError:
            pass
