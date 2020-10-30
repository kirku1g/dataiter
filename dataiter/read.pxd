# cython: language_level=3, boundscheck=False
from dataiter cimport buffer as bf


cdef class base_reader:
    cdef:
        public object dtype
        public Py_ssize_t count
        public Py_ssize_t pos
        public Py_ssize_t stop
        bint _array
        object _callback
        bint _callback_exists
        Py_ssize_t _itemsize
        Py_ssize_t _prev_pos
    cdef _create_array(self, obj)
    cpdef all(self)
    cpdef first(self)
    cpdef read(self, Py_ssize_t read_count)
    cpdef seek(self, Py_ssize_t pos, how=?)

cdef class data_reader(base_reader):
    cdef:
        object _data
    cpdef read(self, Py_ssize_t read_count)
    cpdef seek(self, Py_ssize_t pos, how=?)

cdef class iterable_reader(base_reader):
    cdef:
        bf.DataBufferUint8 _buffer
        object _data_iter
    cpdef read(self, Py_ssize_t read_count)

cdef class file_reader(base_reader):
    cdef:
        public object name
        object fileobj
        object filelike
    cpdef read(self, Py_ssize_t read_count)
    cpdef seek(self, Py_ssize_t pos, how=?)
