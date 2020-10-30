# cython: language_level=3, boundscheck=False
import io
import os
import zipfile

cimport cython
import numpy as np

from dataiter.compression import open_compressed
from dataiter cimport buffer as bf, types
from dataiter import iterate as it


# default read size in bytes, code using this module assumes this is a multiple of megabytes
cdef Py_ssize_t READ_SIZE_C = 16 * 1024 * 1024
READ_SIZE = READ_SIZE_C


cdef class base_reader:
    '''
    Abstract class for reading data incrementally from a source. Subclasses provide a consistent interface for reading
    data from multiple sources, e.g. files, streams, memoryviews and iterables.
    '''
    def __init__(self, dtype=False, Py_ssize_t count=-1, Py_ssize_t start=0, Py_ssize_t stop=0, bint byte=True,
                 bint array=False, callback=None):
        '''
        :param dtype: dtype to read. None specifies bytes/unsigned char, False specifies leaving source dtype unchanged.
        :type dtype: bool or None or np.dtype
        :param start: start position to read from
        :param stop: stop position to read until
        :param byte: whether or not start and stop positions are in bytes or dtype itemsize (for consistency with seek)
        :param array: whether or not to output arrays (for python) or memoryview compatible (faster for cython)
        :param callback: callback function to call each read with current position (e.g. for UI feedback)
        '''
        if start < 0:
            raise ValueError('start must be 0 (default) or positive')
        if stop < 0:
            raise ValueError('stop must be 0 (default - continue until the end) or positive')
        if stop and stop <= start:
            raise ValueError('stop must be greater than start')

        self.dtype = dtype and np.dtype(dtype)
        if not self.dtype and array:
            raise ValueError('dtype must be provided to return specific array type')
        self._array = array
        self._itemsize = self.dtype.itemsize if dtype else 1

        if count == -1:
            count = READ_SIZE_C // self._itemsize
        elif count <= 0:
            raise ValueError('count must be positive')

        if self._itemsize != 1 and not byte:
            start *= self._itemsize
            stop *= self._itemsize

        self.count = count
        self.pos = start
        self._prev_pos = self.pos
        self.stop = stop
        self._callback_exists = callback is not None
        if self._callback_exists:
            self._callback = callback

    cdef _create_array(self, obj):
        '''
        Creates an array if self.array is True.
        '''
        return types.as_array(obj, self.dtype) if self._array else obj

    @cython.cdivision(True)
    def __next__(self):
        '''
        Read count of items from source, raises StopIteration if there is no more data to read.
        '''
        cdef Py_ssize_t read_count

        if self.stop:
            read_count = (self.stop - self.pos) // self._itemsize
            if read_count > self.count:
                read_count = self.count
        else:
            read_count = self.count

        if read_count <= 0:
            raise StopIteration

        data = self.read(read_count)

        cdef Py_ssize_t data_size = len(data)
        if not data_size:
            raise StopIteration

        self.pos += data_size * self._itemsize
        if self._callback_exists and self.pos != self._prev_pos:  # TODO: callback progress
            self._callback(self.pos)
        self._prev_pos = self.pos
        return data

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass

    def __iter__(self):
        return self

    cpdef all(self):
        '''
        Read all data incrementally from source.
        '''
        with self as data_iter:
            return it.join(self)

    cpdef first(self):
        '''
        Read the first count of items from source.
        '''
        with self as data_iter:
            return next(iter(data_iter), None)

    cpdef read(self, Py_ssize_t count):
        '''
        Read a count of items from source.
        '''
        raise NotImplementedError()

    cpdef seek(self, Py_ssize_t pos, how=os.SEEK_SET):
        '''
        Seek to a byte position within source.
        '''
        raise NotImplementedError('seek not supported')


cdef class data_reader(base_reader):
    '''
    Read incrementally from data.
    '''
    def __init__(self, data, *args, **kwargs):
        '''
        :param data: data to read from
        :type data: types.is_data compatible
        '''
        super().__init__(*args, **kwargs)
        if self.dtype is not False:
            data_dtype = types.get_dtype(data)
            if data_dtype != self.dtype:
                data = types.view_dtype(data, self.dtype)
        self._data = data

    @cython.cdivision(True)
    cpdef read(self, Py_ssize_t count):
        '''
        Read count of items from data.
        '''
        return self._create_array(self._data[self.pos // self._itemsize:(self.pos // self._itemsize) + count])

    @cython.cdivision(True)
    cpdef seek(self, Py_ssize_t pos, how=os.SEEK_SET):
        '''
        Seek to a byte position in source.
        '''
        if pos % self._itemsize:
            raise ValueError('cannot seek to byte position within data as it is not a multiple of itemsize')

        self.pos = pos


cdef class iterable_reader(base_reader):
    '''
    Read data incrementally from a data iterable.
    '''
    def __init__(self, data_iter, *args, **kwargs):
        '''
        :param data_iter: data iterable
        :type data_iter: iterable
        '''
        super().__init__(*args, **kwargs)
        self._buffer = bf.DataBufferUint8()
        self._data_iter = it.iter_view_dtype(data_iter, np.uint8)
        cdef:
            Py_ssize_t current_pos = 0, next_pos
        if self.pos:
            for data in self._data_iter:
                next_pos = current_pos + len(data)
                if next_pos == self.pos:
                    break
                elif next_pos > self.pos:
                    self._buffer.add(data[self.pos - current_pos:])
                    break
                current_pos = next_pos

    cpdef read(self, Py_ssize_t count):
        '''
        Read count of items from iterable.
        '''
        cdef Py_ssize_t size = count * self._itemsize
        while self._buffer.size < size:
            try:
                self._buffer.add(next(self._data_iter))
            except StopIteration:
                break

        return self._create_array(types.view_dtype(self._buffer.read(size), self.dtype))


cdef class file_reader(base_reader):
    '''
    Read data incrementally from a file path or fileobj.
    '''
    def __init__(self, filelike, *args, **kwargs):
        '''
        :param filelike: file path or fileobj
        :type filelike: str or pathlib.Path or fileobj
        '''
        kwargs['dtype'] = kwargs.get('dtype') or None  # change False to None
        super().__init__(**kwargs)
        self.filelike = filelike

    def __enter__(self):
        '''
        Opens file for reading.
        '''
        # TODO: Handle '-' as stdin, e.g. getattr(sys.stdin, 'buffer', sys.stdin)?
        if hasattr(self.filelike, 'read') and callable(self.filelike.read):
            self.fileobj = self.filelike
        else:
            self.fileobj = open_compressed(self.filelike)

        if self.pos:
            try:
                self.fileobj.seek(self.pos)
            except zipfile.io.UnsupportedOperation:
                self.fileobj.read(self.pos)  # read as seek is not supported
        return self

    def __exit__(self, *exc_info):
        '''
        Closes file if supported.
        '''
        if not getattr(self.fileobj, 'closed', True):
            self.fileobj.close()

    @cython.cdivision(True)
    cpdef read(self, Py_ssize_t read_count):
        '''
        Read count of items from file.
        '''
        if self.fileobj is None:
            raise RuntimeError(f'file not opened - {self.__class__.__name__} must be used as a context manager')

        if self.dtype is not None and isinstance(self.fileobj, io.BufferedReader):
            kwargs = {'dtype': self.dtype}
            if read_count:
                kwargs['count'] = read_count
            data = np.fromfile(self.fileobj, **kwargs)
        else:
            data = self.fileobj.read(read_count * self._itemsize) if read_count else self.fileobj.read()
            if self.dtype is not None:
                if self._itemsize != 1:
                    remainder = len(data) % self._itemsize
                    if remainder:
                        data = data[:-remainder]
                data = np.frombuffer(data, dtype=self.dtype)
        return data

    cpdef seek(self, Py_ssize_t pos, how=os.SEEK_SET):
        '''
        Seek to a byte position in source.

        :raises zipfile.io.UnsupportedOperation: Files opened within zip files do not support seek.
        '''
        if self.fileobj is None:
            raise RuntimeError(f'file not opened - {self.__class__.__name__} must be used as a context manager')

        self.fileobj.seek(pos, how)
        self.pos = pos


def reader(obj, *args, **kwargs):
    '''
    reader factory which creates a reader based on the type of object.

    :type obj: base_reader subclass object, bytes, str, np.array or generator
    :rtype: base_reader subclass object
    '''
    if types.is_data(obj):
        cls = data_reader
    elif types.is_data_iterable(obj):
        cls = iterable_reader
    else:
        cls = file_reader  # filepath or fileobj
    return cls(obj, *args, **kwargs)


def read(data):
    '''
    Helper generator which enters and exits reader as a context manager (required for opening and closing files).
    '''
    with reader(data) as r:
        yield from r

