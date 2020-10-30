# cython: language_level=3, boundscheck=False
'''
Functions for data iterables.
'''

##############################################################################
# Imports


cimport cython
import numpy as np

from dataiter cimport buffer as bf, types


##############################################################################
# Functions


def chunk(data_iter, Py_ssize_t size, bint flush=False):
    '''
    Chunk any data iterable into chunks of a specific size.

    Unlike chunk_dtype and chunk_uint8 an iterator is required as the dtype of the underlying buffer is inferred from
    the first element of the iterator.

    :dtype data_iter: iterator
    '''
    if size <= 0:
        raise ValueError('size must be greater than 0')

    data_iter = iter(data_iter)
    try:
        first_data = next(data_iter)
    except StopIteration:
        return
    return chunk_dtype(prepend(first_data, data_iter), size, types.get_dtype(first_data), flush=flush)


def chunk_dtype(data_iter, Py_ssize_t size, dtype=None, bint flush=False):
    '''
    Chunk a dtype data iterable into chunks of a specific size.
    '''
    cdef bint uint8 = types.dtype_uint8(dtype)
    if not uint8:
        data_iter = iter_view_dtype(data_iter, np.uint8)
        size *= types.get_itemsize(dtype)

    data_iter = chunk_uint8(data_iter, size, flush=flush)

    if not uint8:
        data_iter = iter_view_dtype(data_iter, dtype)

    return data_iter


def chunk_uint8(data_iter, Py_ssize_t size, bint flush=False):
    '''
    Chunk a uint8 data iterable into chunks of a specific size.
    '''
    if size <= 0:
        raise ValueError('size must be greater than 0')

    cdef bf.DataBufferUint8 buff = bf.DataBufferUint8()
    for data in data_iter:
        buff.add(data)
        while buff.size >= size:
            d = buff.read(size)
            yield d

    if flush and buff.size:
        d = buff.read(size)
        yield d


def join(chunks, dtype=None):
    '''
    Join chunks of the same type of data together.

    :param chunks: Chunks of data to join together.
    :type chunks: iterable of str or array
    :param dtype: dtype to return if chunks is empty.
    :type dtype: np.dtype or None
    :returns: Chunks of data joined into a single object.
    :rtype: str or np.ndarray
    '''
    chunks = tuple(iter_data(chunks))
    if chunks:
        return types.as_dtype(np.concatenate([memoryview(c) for c in chunks]), types.get_dtype(chunks[0]))
    else:
        return np.empty(0, dtype=dtype) if dtype else b''


def iter_as_dtype(data_iter, dtype):
    '''
    Iterate through a data iterable and yield data as dtype (casts if required).

    :type data_iter: iterable
    :yields: data as dtype
    '''
    return (types.as_dtype(data, dtype) for data in data_iter)


def iter_data(data_iter):
    '''
    Iterate through an iterable and yield only data.

    :type data_iter: iterable
    :yields: data
    '''
    return (d for d in data_iter if types.is_data(d))


def iter_data_slice(data_iter, slice):
    '''
    Yield a data slice for every piece of data in the iterable.

    :type data_iter: data iterable
    :type slice: slice
    '''
    return (data[slice] for data in data_iter)


def iter_data_slices(data_iter, slices):
    '''
    Yield a list of data slices for each piece of data in the iterable.

    :type data_iter: data iterable
    :type slices: iterable of slice
    '''
    return ([data[s] for s in slices] for data in data_iter)


def iter_data_start_idx(data_iter, Py_ssize_t start):
    '''
    Iterate through a data iterable and skip data until reaching the start index.

    :type data_iter: data iterable
    '''
    if start <= 0:
        yield from data_iter
        return

    cdef Py_ssize_t data_size, pos = 0, next_pos

    for data in data_iter:
        data_size = len(data)
        next_pos = pos + data_size
        if next_pos == start:
            break
        elif next_pos > start:
            yield from chunk(prepend(data[start - pos:], data_iter), data_size, flush=True)
            return
        pos = next_pos

    yield from data_iter


def iter_data_stop_idx(data_iter, Py_ssize_t stop):
    '''
    Iterate through a data iterable and yield until reaching the stop index.

    :type data_iter: data iterable
    '''
    if stop <= 0:
        return
    cdef Py_ssize_t pos = 0, next_pos
    for data in data_iter:
        next_pos = pos + len(data)
        if next_pos > stop:
            yield data[:stop - pos]
            return
        yield data
        if next_pos == stop:
            return
        pos = next_pos


def iter_view_dtype(data_iter, dtype):
    '''
    Iterate through a data iterable and yield the data viewed as dtype.

    :type data_iter: iterable
    :yields: data viewed as dtype
    '''
    return data_iter if dtype is False else (types.view_dtype(data, dtype) for data in data_iter)


def prepend(data, data_iter):
    '''
    Prepend a data iterable with a piece of data (e.g. with the remainder of a variable size header).

    :type data_iter: iterable
    '''
    yield data
    yield from data_iter

