# cython: language_level=3
from cpython cimport bytearray, PySequence_Length, PySequence_InPlaceConcat, PyUnicode_AsUTF8String
from libc.stdint cimport uint64_t, int64_t, uint32_t, int32_t

# {{{ definitions

cdef extern from "Python.h":
    Py_ssize_t PyByteArray_GET_SIZE(object array)
    object PyUnicode_FromStringAndSize(char *buff, Py_ssize_t len)
    object PyBytes_FromStringAndSize(char *buff, Py_ssize_t len)
    object PyString_FromStringAndSize(char *buff, Py_ssize_t len)
    int PyByteArray_Resize(object self, Py_ssize_t size) except -1
    char* PyByteArray_AS_STRING(object bytearray)

ctypedef object(*Decoder)(char **pointer, char *end)

class InternalDecodeError(Exception):
    pass

cdef inline object makeDecodeError(char* pointer, message):
    cdef uint64_t locator = <uint64_t>pointer
    return InternalDecodeError(locator, message)

class DecodeError(Exception):
    def __init__(self, pointer, message):
        self.pointer = pointer
        self.message = message
    def __str__(self):
        return self.message.format(self.pointer)

# }}}

# {{{ decoding

# {{{ raw stuff

cdef inline int raw_decode_uint32(char **start, char *end, uint32_t *result) nogil:
    cdef uint32_t value = 0
    cdef uint32_t byte
    cdef char *pointer = start[0]
    cdef int counter = 0
    while True:
        if pointer >= end:
            return -1
        byte = pointer[0]
        value |= (byte & 0x7f) << counter
        counter+=7
        pointer+=1
        if byte & 0x80 == 0:
            break
    start[0] = pointer
    result[0] = value
    return 0

cdef inline int raw_decode_uint64(char **start, char *end, uint64_t *result) nogil:
    cdef uint64_t value = 0
    cdef uint64_t byte
    cdef char *pointer = start[0]
    cdef int counter = 0

    while True:
        if pointer >= end:
            return -1
        byte = pointer[0]
        value |= (byte & 0x7f) << counter
        counter+=7
        pointer+=1
        if byte & 0x80 == 0:
            break
    start[0] = pointer
    result[0] = value
    return 0

cdef inline int raw_decode_fixed32(char **pointer, char *end, uint32_t *result) nogil:
    cdef uint32_t value = 0
    cdef char *start = pointer[0]
    cdef int i

    for i from 0 <= i < 4:
        if start == end:
            return -1
        value |= <unsigned char>start[0] << (i * 8)
        start += 1
    pointer[0] = start
    result[0] = value
    return 0

cdef inline int raw_decode_fixed64(char **pointer, char *end, uint64_t *result) nogil:
    cdef uint64_t value = 0
    cdef char *start = pointer[0]
    cdef uint64_t temp = 0
    cdef int i
    for i from 0 <= i < 8:
        if start == end:
            return -1
        temp = <unsigned char>start[0]
        value |= temp << (i * 8)
        start += 1
    pointer[0] = start
    result[0] = value
    return 0

cdef inline int raw_decode_delimited(char **pointer, char *end, char **result, uint64_t *size) nogil:
    if raw_decode_uint64(pointer, end, size):
        return -1

    cdef char* start = pointer[0]
    if start+size[0] > end:
        return -2

    result[0] = start
    pointer[0] = start+size[0]
    return 0

# }}}

cdef object decode_uint32(char **pointer, char *end):
    cdef uint32_t result
    if raw_decode_uint32(pointer, end, &result):
        raise makeDecodeError(pointer[0], "Can't decode value of type `uint32` at [{0}]")

    return result

cdef object decode_uint64(char **pointer, char *end):
    cdef uint64_t result
    if raw_decode_uint64(pointer, end, &result):
        raise makeDecodeError(pointer[0], "Can't decode value of type `uint64` at [{0}]")

    return result

cdef object decode_int32(char **pointer, char *end, ):
    cdef int32_t result
    if raw_decode_uint32(pointer, end, <uint32_t*>&result):
        raise makeDecodeError(pointer[0], "Can't decode value of type `int32` at [{0}]")

    return result

cdef object decode_int64(char **pointer, char *end, ):
    cdef int64_t result
    if raw_decode_uint64(pointer, end, <uint64_t*>&result):
        raise makeDecodeError(pointer[0], "Can't decode value of type `int64` at [{0}]")

    return result

cdef object decode_sint32(char **pointer, char *end, ):
    cdef uint32_t result
    if raw_decode_uint32(pointer, end, &result):
        raise makeDecodeError(pointer[0], "Can't decode value of type `sint32` at [{0}]")

    return <int32_t>((result >> 1) ^ (-<int32_t>(result & 1)))

cdef object decode_sint64(char **pointer, char *end, ):
    cdef uint64_t un
    if raw_decode_uint64(pointer, end, &un):
        raise makeDecodeError(pointer[0], "Can't decode value of type `sint64` at [{0}]")

    return <int64_t>((un >> 1) ^ (-<int64_t>(un & 1)))

cdef object decode_fixed32(char **pointer, char *end, ):
    cdef uint32_t result
    if raw_decode_fixed32(pointer, end, &result):
        raise makeDecodeError(pointer[0], "Can't decode value of type `fixed32` at [{0}]")

    return result

cdef object decode_fixed64(char **pointer, char *end, ):
    cdef uint64_t result
    if raw_decode_fixed64(pointer, end, &result):
        raise makeDecodeError(pointer[0], "Can't decode value of type `fixed64` at [{0}]")

    return result

cdef object decode_sfixed32(char **pointer, char *end, ):
    cdef int32_t result
    if raw_decode_fixed32(pointer, end, <uint32_t*>&result):
        raise makeDecodeError(pointer[0], "Can't decode value of type `sfixed32` at [{0}]")

    return result

cdef object decode_sfixed64(char **pointer, char *end, ):
    cdef int64_t result
    if raw_decode_fixed64(pointer, end, <uint64_t*>&result):
        raise makeDecodeError(pointer[0], "Can't decode value of type `sfixed64` at [{0}]")

    return result

cdef object decode_bytes(char **pointer, char *end, ):
    cdef char *result
    cdef uint64_t size
    cdef int ret = raw_decode_delimited(pointer, end, &result, &size)
    if ret==0:
        return PyBytes_FromStringAndSize(result, size)

    if ret == -1:
        raise makeDecodeError(pointer[0], "Can't decode size for value of type `bytes` at [{0}]")
    elif ret == -2:
        raise makeDecodeError(pointer[0], "Can't decode value of type `bytes` of size %d at [{0}]" % size)


cdef object decode_string(char **pointer, char *end, ):
    cdef char *result
    cdef uint64_t size
    cdef int ret = raw_decode_delimited(pointer, end, &result, &size)
    if ret==0:
        return PyUnicode_FromStringAndSize(result, size)

    if ret == -1:
        raise makeDecodeError(pointer[0], "Can't decode size for value of type `string` at [{0}]")
    elif ret == -2:
        raise makeDecodeError(pointer[0], "Can't decode value of type `string` of size %d at [{0}]" % size)

cdef object decode_float(char **pointer, char *end, ):
    cdef float result
    if raw_decode_fixed32(pointer, end, <uint32_t*>&result):
        raise makeDecodeError(pointer[0], "Can't decode value of type `float` at [{0}]")

    return result

cdef object decode_double(char **pointer, char *end, ):
    cdef double result
    if raw_decode_fixed64(pointer, end, <uint64_t*>&result):
        raise makeDecodeError(pointer[0], "Can't decode value of type `double` at [{0}]")

    return result

cdef object decode_bool(char **pointer, char *end, ):
    cdef char* start = pointer[0]
    pointer[0] = start + 1

    return <bint>start[0]

# }}}

# {{{ encoding

cdef inline int raw_encode_uint32(bytearray array, uint32_t n) except -1:
    cdef unsigned short int rem
    cdef Py_ssize_t size = PyByteArray_GET_SIZE(array)
    PyByteArray_Resize(array, size + 10)
    cdef char *buff = PyByteArray_AS_STRING(array) + size

    if 0!=n:
        while True:
            rem = <char>(n & 0x7f)
            n = n>>7
            if 0==n:
                buff[0] = <char> rem
                buff+=1
                break
            else:
                rem = rem | 0x80
                buff[0] = <char> rem
                buff+=1
    else:
        buff[0] = b'\0'
        buff+=1

    PyByteArray_Resize(array, buff - PyByteArray_AS_STRING(array))
    return 0

cdef inline int raw_encode_uint64(bytearray array, uint64_t n) except -1:
    cdef unsigned short int rem
    cdef Py_ssize_t size = PyByteArray_GET_SIZE(array)
    PyByteArray_Resize(array, size + 20)
    cdef char *buff = PyByteArray_AS_STRING(array) + size

    if 0!=n:
        while True:
            rem = <char>(n & 0x7f)
            n = n>>7
            if 0==n:
                buff[0] = <char> rem
                buff+=1
                break
            else:
                rem = rem | 0x80
                buff[0] = <char> rem
                buff+=1
    else:
        buff[0] = b'\0'
        buff+=1
    PyByteArray_Resize(array, buff - PyByteArray_AS_STRING(array))
    return 0

cdef inline int raw_encode_fixed32(bytearray array, uint32_t n) except -1:
    cdef unsigned short int rem
    cdef Py_ssize_t size = PyByteArray_GET_SIZE(array)
    PyByteArray_Resize(array, size + 4)
    cdef char *buff = PyByteArray_AS_STRING(array) + size
    cdef int i

    for i from 0 <= i < 4:
        rem = n & 0xff
        n = n >> 8
        buff[0] = <char> rem
        buff += 1

    return 0

cdef inline int raw_encode_fixed64(bytearray array, uint64_t n) except -1:
    cdef unsigned short int rem
    cdef Py_ssize_t size = PyByteArray_GET_SIZE(array)
    PyByteArray_Resize(array, size + 8)
    cdef char *buff = PyByteArray_AS_STRING(array) + size
    cdef int i

    for i from 0 <= i < 8:
        rem = n & 0xff
        n = n >> 8
        buff[0] = <char> rem
        buff += 1

    return 0

cdef inline encode_string(bytearray array, object n):
    if isinstance(n, unicode):
        n = PyUnicode_AsUTF8String(n)
    cdef Py_ssize_t len = PySequence_Length(n)
    raw_encode_uint64(array, len)
    PySequence_InPlaceConcat(array, n)

cdef inline encode_float(bytearray array, object value):
    cdef float f = value
    raw_encode_fixed32(array, (<uint32_t*>&f)[0])

# }}}
