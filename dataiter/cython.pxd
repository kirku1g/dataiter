# cython: language_level=3, boundscheck=False, wraparound=False
'''
TODO:
- add const to fused type functions when supported by Cython for immutable buffers
'''
cimport cython
cimport numpy as np

'''
A fused type allows for a 'statically' typed function to accept multiple different types, e.g.
cpdef operation(np_types[:] data, np_types value)
https://cython.readthedocs.io/en/latest/src/userguide/fusedtypes.html

Limitations:
- Fused types do not currenty support the const keyword for numpy types, either within the ctypedef fused section, the
  cython.fused_type(...) function or within function signatures, e.g. cdef func(const np_types x). This is a shame as it precludes
  compatibility with constant buffers, e.g. bytes objects.
- Fused types do not allow numpy types to be conformed into C types. Whereas cdef func(unsigned char value) will automatically
  conform np.uint8_t into the corresponding C type, a fused type containing unsigned char does not accept np.uint8_t.
- Fused types do not support multiple types which represent the same underlying C type, e.g. a fused type is not usable if it
  contains both np.uint8_t and unsigned char.
- When calling a function from Python, the type of a fused type variable is inferred automatically. When calling from Cython,
  input variables must be statically typed, otherwise compilation will fail due to the underlying C function call being ambiguous.
- If multiple different type signatures of the same function must be called within the same scope, the specific signature must
  be specified, e.g. myfunc[double](<double>value); myfunc[float](<float>value)
'''
ctypedef fused np_types:
    np.uint8_t
    np.uint16_t
    np.uint32_t
    np.uint64_t
    np.int8_t
    np.int16_t
    np.int32_t
    np.int64_t
    np.intp_t
    np.float32_t
    np.float64_t

cdef Py_ssize_t NONE_IDX

################################################################################
# Memory Allocation
cdef np.int8_t[:] empty_int8(np.intp_t psize)
cdef np.int8_t[:, :] empty2d_int8(np.intp_t x, np.intp_t y)
cdef np.int8_t[:] zeros_int8(np.intp_t psize)
cdef np.int8_t[:, :] zeros2d_int8(np.intp_t x, np.intp_t y)
cdef np.int16_t[:] empty_int16(np.intp_t psize)
cdef np.int16_t[:, :] empty2d_int16(np.intp_t x, np.intp_t y)
cdef np.int16_t[:] zeros_int16(np.intp_t psize)
cdef np.int16_t[:, :] zeros2d_int16(np.intp_t x, np.intp_t y)
cdef np.int32_t[:] empty_int32(np.intp_t psize)
cdef np.int32_t[:, :] empty2d_int32(np.intp_t x, np.intp_t y)
cdef np.int32_t[:] zeros_int32(np.intp_t psize)
cdef np.int32_t[:, :] zeros2d_int32(np.intp_t x, np.intp_t y)
cdef np.int64_t[:] empty_int64(np.intp_t psize)
cdef np.int64_t[:, :] empty2d_int64(np.intp_t x, np.intp_t y)
cdef np.int64_t[:] zeros_int64(np.intp_t psize)
cdef np.int64_t[:, :] zeros2d_int64(np.intp_t x, np.intp_t y)
cdef np.intp_t[:] empty_intp(np.intp_t psize)
cdef np.intp_t[:, :] empty2d_intp(np.intp_t x, np.intp_t y)
cdef np.intp_t[:] zeros_intp(np.intp_t psize)
cdef np.intp_t[:, :] zeros2d_intp(np.intp_t x, np.intp_t y)
cdef np.intp_t[:, :, :] zeros3d_intp(np.intp_t x, np.intp_t y, np.intp_t z)
cdef np.uint8_t[:] empty_uint8(np.intp_t size)
cdef np.uint8_t[:, :] empty2d_uint8(np.intp_t x, np.intp_t y)
cdef np.uint8_t[:] zeros_uint8(np.intp_t size)
cdef np.uint8_t[:, :] zeros2d_uint8(np.intp_t x, np.intp_t y)
cdef np.uint8_t[:] ones_uint8(np.intp_t size)
cdef np.uint16_t[:] empty_uint16(np.intp_t size)
cdef np.uint16_t[:, :] empty2d_uint16(np.intp_t x, np.intp_t y)
cdef np.uint16_t[:] zeros_uint16(np.intp_t size)
cdef np.uint16_t[:, :] zeros2d_uint16(np.intp_t x, np.intp_t y)
cdef np.uint32_t[:] empty_uint32(np.intp_t size)
cdef np.uint32_t[:, :] empty2d_uint32(np.intp_t x, np.intp_t y)
cdef np.uint32_t[:] zeros_uint32(np.intp_t size)
cdef np.uint32_t[:, :] zeros2d_uint32(np.intp_t x, np.intp_t y)
cdef np.uint64_t[:] empty_uint64(np.intp_t size)
cdef np.uint64_t[:, :] empty2d_uint64(np.intp_t x, np.intp_t y)
cdef np.uint64_t[:] zeros_uint64(np.intp_t size)
cdef np.uint64_t[:, :] zeros2d_uint64(np.intp_t x, np.intp_t y)
cdef np.float64_t[:] empty_float64(np.intp_t size)
cdef np.float64_t[:, :] empty2d_float64(np.intp_t x, np.intp_t y)
cdef np.float64_t[:] zeros_float64(np.intp_t size)
cdef np.float64_t[:, :] zeros2d_float64(np.intp_t x, np.intp_t y)
################################################################################
# Unpacking data types
cdef np.uint16_t unpack_uint16_le_unsafe(const np.uint8_t[:] data, Py_ssize_t idx=?) nogil
cdef np.uint16_t unpack_uint16_le(const np.uint8_t[:] data, Py_ssize_t idx=?) nogil
cdef np.uint16_t unpack_uint16_be_unsafe(const np.uint8_t[:] data, Py_ssize_t idx=?) nogil
cdef np.uint16_t unpack_uint16_be(const np.uint8_t[:] data, Py_ssize_t idx=?) nogil
cdef np.uint32_t unpack_uint32_le_unsafe(const np.uint8_t[:] data, Py_ssize_t idx=?) nogil
cdef np.uint32_t unpack_uint32_le(const np.uint8_t[:] data, Py_ssize_t idx=?) nogil
cdef np.uint32_t unpack_uint32_be_unsafe(const np.uint8_t[:] data, Py_ssize_t idx=?) nogil
cdef np.uint32_t unpack_uint32_be(const np.uint8_t[:] data, Py_ssize_t idx=?) nogil
################################################################################
# Packing data types
cdef void pack_uint32_be_unsafe(np.uint8_t[:] data, np.uint32_t value, Py_ssize_t idx=?) nogil
################################################################################
# Endianness
cdef np.uint16_t byteswap_uint16(np.uint16_t value) nogil
cdef np.int16_t byteswap_int16(np.uint16_t value) nogil
cdef np.uint32_t byteswap_uint32(np.uint32_t value) nogil
cdef np.int32_t byteswap_int32(np.uint32_t value) nogil
cdef np.float32_t byteswap_float32(np.float32_t value) nogil
################################################################################
# Array helpers
cdef astype(data, dtype=?, copy=?)
cdef bint lengths_match(Py_ssize_t x, Py_ssize_t y) nogil
cdef bint within_bounds(Py_ssize_t idx, Py_ssize_t length) nogil
cdef idx_none(Py_ssize_t idx)
cdef Py_ssize_t none_idx(idx)
cdef Py_ssize_t array_wraparound_idx(Py_ssize_t idx, Py_ssize_t length, bint stop=?) nogil
cdef array_idx_value(Py_ssize_t idx, array)
################################################################################
# Array index finders
cdef Py_ssize_t prev_idx_unsafe(const np.uint8_t[:] array, Py_ssize_t idx, bint match=?, Py_ssize_t start=?) nogil
cdef Py_ssize_t prev_idx(const np.uint8_t[:] array, Py_ssize_t idx, bint match=?, Py_ssize_t start=?) nogil
cdef Py_ssize_t next_idx_unsafe(const np.uint8_t[:] array, Py_ssize_t idx=?, bint match=?, Py_ssize_t stop=?) nogil
cdef Py_ssize_t next_idx(const np.uint8_t[:] array, Py_ssize_t idx=?, bint match=?, Py_ssize_t stop=?) nogil
cdef Py_ssize_t first_idx(const np.uint8_t[:] array, bint match=?) nogil
cdef Py_ssize_t last_idx(const np.uint8_t[:] array, bint match=?) nogil
cdef Py_ssize_t nearest_idx_unsafe(const np.uint8_t[:] array, Py_ssize_t idx, bint match=?, Py_ssize_t start=?,
                                   Py_ssize_t stop=?) nogil
cdef Py_ssize_t nearest_idx(const np.uint8_t[:] array, Py_ssize_t idx, bint match=?, Py_ssize_t start=?,
                            Py_ssize_t stop=?) nogil
cdef Py_ssize_t subarray_idx_uint8(const np.uint8_t[:] array, const np.uint8_t[:] subarray, Py_ssize_t start=?) nogil
cdef Py_ssize_t value_idx(np_types[:] array, np_types value) nogil
cdef Py_ssize_t value_idx_uint8(const np.uint8_t[:] array, np.uint8_t value) nogil
################################################################################
# Array operations
cdef np_types arrays_continuous_value(np_types[:] data1, np_types[:] data2, Py_ssize_t idx) nogil
cdef np.uint16_t[:] byteswap_array(const np.uint8_t[:] data)
cdef np.uint8_t[:] contract_runs(np.uint8_t[:] data, Py_ssize_t size, bint match=?) nogil
cdef np.uint8_t[:] remove_small_runs(np.uint8_t[:] data, Py_ssize_t size, bint match=?) nogil
cdef np.uint8_t[:] remove_small_runs_hz(np.uint8_t[:] data, np.float64_t seconds, np.float64_t hz=?, bint match=?) nogil
################################################################################
# Concatenation
cdef np.uint8_t[:] concatenate_uint8(memviews)