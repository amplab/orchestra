# unison: fast, space efficient and backward compatible python serialization
#
# This module exports:
#
# unison.serialize(bytearray, object): Serialize a python object
#   into a bytearray
# unison.deserialize(bytes, schema): Deserialize a python object
#   with a given schema from a byte string
#
# The schema can be a python type, or an object of the form List[schema],
# Tuple[schema1, schema2, ...]
#
# See runtest.py for examples

import cprotobuf
import orchpy.protos_pb as pb
import numpy as np

include "utils.pxi"

class TypeAlias(object):
  """Class for defining generic aliases for library types."""

  def __init__(self, target_type):
    self.target_type = target_type

  def __getitem__(self, typeargs):
    return (self.target_type, typeargs)

List = TypeAlias(list)
# Dict = TypeAlias(dict)
# Set = TypeAlias(set)
Tuple = TypeAlias(tuple)
# Callable = TypeAlias(callable)

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

cpdef serialize(bytearray buf, val):
  if type(val) == int or type(val) == long:
    raw_encode_uint64(buf, <uint64_t>val)
  elif type(val) == float:
    encode_float(buf, val)
  elif type(val) == str or type(val) == unicode:
    encode_string(buf, val)
  elif type(val) == tuple:
    for elem in val:
      serialize(buf, elem)
  elif type(val) == list:
    serialize(buf, len(val))
    for elem in val:
      serialize(buf, elem)
  elif type(val) == np.ndarray:
    proto = array_to_proto(val)
    data = proto.SerializeToString()
    serialize(buf, len(data))
    buf.extend(data)
  else:
    data = val.proto.SerializeToString()
    serialize(buf, len(data))
    buf.extend(data)

cdef object deserialize_primitive(char **buff, char *end, type t):
  if t == int or t == long:
    return decode_uint64(buff, end)
  if t == float:
    return decode_float(buff, end)
  if t == str or t == unicode:
    return decode_string(buff, end)
  if t == np.ndarray:
    size = deserialize_primitive(buff, end, int)
    data = PyString_FromStringAndSize(buff[0], size)
    buff[0] += size
    array = pb.Array()
    array.ParseFromString(data)
    return proto_to_array(array)
  else:
    size = deserialize_primitive(buff, end, int)
    data = PyString_FromStringAndSize(buff[0], size)
    buff[0] += size
    result = t()
    result.deserialize(data)
    return result


cdef object deserialize_buffer(char **buff, char *end, schema):
  if type(schema) == type:
    return deserialize_primitive(buff, end, schema)
  if type(schema) == tuple and schema[0] == tuple:
    result = []
    for t in schema[1]:
      result.append(deserialize_buffer(buff, end, t))
    return tuple(result)
  if type(schema) == tuple and schema[0] == list:
    result = []
    len = deserialize_primitive(buff, end, long)
    for i in range(len):
      result.append(deserialize_buffer(buff, end, schema[1]))
    return result

cpdef object deserialize(bytes data, schema):
  cdef char *buff = <char*>data
  cdef Py_ssize_t size = len(data)
  cdef char *end = buff + size
  return deserialize_buffer(&buff, end, schema)
