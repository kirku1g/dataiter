# cython: language_level=3, boundscheck=False
cimport numpy as np


cdef class WriteBufferUint8:
    cdef:
        public np.uint8_t[:] buffer
        public Py_ssize_t size
    cdef inline Py_ssize_t free(self) nogil
    cdef void write(self, data) except *
    cdef void write_uint8(self, const np.uint8_t[:] data) except *
    cdef np.uint8_t[:] writable_uint8(self, Py_ssize_t size, bint zero=?)
    cdef np.uint8_t[:] flush(self) nogil

cdef class WriteBufferUint16:
    cdef:
        public np.uint16_t[:] buffer
        public Py_ssize_t size
    cdef inline Py_ssize_t free(self) nogil
    cdef void write(self, data) except *
    cdef void write_uint16(self, const np.uint16_t[:] data) except *
    cdef void write_uint8(self, const np.uint8_t[:] data, bint byteswap=?) except *
    cdef np.uint16_t[:] writable_uint16(self, Py_ssize_t size)
    cdef np.uint16_t[:] flush(self)

cdef class DataBufferUint8:
    cdef:
        object _chunks
        const np.uint8_t[:] _head
        Py_ssize_t _head_idx
        public Py_ssize_t size
    cdef inline Py_ssize_t _head_size(self) nogil
    cdef Py_ssize_t _pop(self)
    cdef inline const np.uint8_t[:] _read_head(self, Py_ssize_t size) nogil
    cdef inline void _truncate_head(self, Py_ssize_t size) nogil
    cpdef clear(self)
    cpdef add(self, const np.uint8_t[:] data)
    cpdef const np.uint8_t[:] read(self, Py_ssize_t size)
    cpdef const np.uint8_t[:] peek(self, Py_ssize_t size)
    cpdef truncate(self, Py_ssize_t size)
