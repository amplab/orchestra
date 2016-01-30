import numpy as np
import orchpy as op
from cprotobuf import ProtoEntity, Field

class ObjRefsProto(ProtoEntity):
  shape = Field('uint64', 1, repeated=True)
  objrefs = Field('bytes', 2, required=False)

class ObjRefs(object):

  def construct(self):
    self.array = np.frombuffer(self.proto.objrefs, dtype=np.dtype('uint64'))
    self.array.shape = self.proto.shape

  def deserialize(self, data):
    self.proto.ParseFromString(data)
    self.construct()

  def from_proto(self, proto):
    self.proto = proto
    self.construct()

  def __init__(self, shape=None):
    self.proto = ObjRefsProto()
    if shape != None:
      self.proto.shape = shape
      self.proto.objrefs = bytearray(np.product(shape) * np.dtype('uint64').itemsize)
      self.construct()

  def __getitem__(self, index):
    result = self.array[index]
    if type(result) == np.uint64:
      return op.ObjRef(result)
    else:
      return np.vectorize(op.ObjRef)(result)

  def __setitem__(self, index, val):
    self.array[index] = val.get_id()
