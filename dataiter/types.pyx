# cython: language_level=3, boundscheck=False
from collections import Sequence

cimport numpy as np
import numpy as np


# XXX: _memoryviewslice is a cython oddity that is not importable.
ARRAY_TYPES = {'MaskedArray', 'ndarray'}
MEMORYVIEW_TYPES = {'memoryview', '_memoryviewslice'}
ARRAY_LIKE_TYPES = ARRAY_TYPES | MEMORYVIEW_TYPES


cdef as_array(obj, dtype):
    if isinstance(obj, np.ndarray):
        return obj if obj.dtype == dtype else obj.view(dtype)
    else:
        return np.frombuffer(obj, dtype)


cdef bytes as_bytes(const np.uint8_t[:] array):
    '''
    Convert a C char array of a specific length (which Cython incorrectly interprets as null-terminated) to bytes.

    When converting char arrays to bytes (e.g. within structs), char arrays are interpreted as null-terminated strings
    where the size of the array is ignored. Casting to a memoryview applies array length.
    '''
    return bytes(array)


cpdef as_dtype(data, dtype):
    if dtype is None:
        return bytes(data)
    elif isinstance(data, np.ndarray):
        return data.astype(dtype)
    else:
        return np.frombuffer(data, dtype=dtype)


cpdef as_int(obj):
    '''
    Returns obj cast as integer, unpacks if wrapped in a sequence.

    :type obj: int, float or list
    :rtype: int
    '''
    try:
        return int(obj)
    except TypeError:
        # list
        if len(obj) == 1:
            return int(obj[0])
        else:
            raise ValueError('single value is required')


cpdef as_sequence(obj, ignore={}):
    '''
    Returns obj wrapped in a tuple if not already a sequence and not None.

    XXX: creating a tuple for a single element is 10x faster than list

    :type obj: None, int, float or list
    :param ignore: sequence types to ignore and wrap in tuple, e.g. str or bytes
    :type ignore: iterable
    :returns: obj unchanged if obj is sequence or None else obj wrapped in a list
    :rtype: collections.Sequence or None
    '''
    return (obj,) if (ignore and type(obj) in ignore) or (not isinstance(obj, Sequence) and obj is not None) else obj


cpdef byte_size(data):
    '''
    Size in bytes of data.

    :type data: bytes, memoryview or np.ndarray
    :rtype: int
    '''
    return data.nbytes if is_array_like(data) else len(data)


cpdef bint dtype_uint8(dtype):
    return dtype in {None, np.uint8}


def _get_dtype_none(obj):
    return None


def _get_dtype_memoryview(obj):
    return np.dtype(obj.format)#getattr(obj.obj, 'dtype', None)


def _get_dtype_memoryviewslice(obj):
    return get_dtype(obj.base)


def _get_dtype_array(obj):
    return obj.dtype


cdef dict GET_DTYPE_FUNCTIONS = {
    'bytearray': _get_dtype_none,
    'bytes': _get_dtype_none,
    'memoryview': _get_dtype_memoryview,
    '_memoryviewslice': _get_dtype_memoryviewslice,
    'ndarray': _get_dtype_array,
    'MaskedArray': _get_dtype_array,
}


cpdef get_dtype(obj):
    '''
    OPT: ~6x faster than worst case of checking type multiple times.
    '''
    return GET_DTYPE_FUNCTIONS[obj.__class__.__name__](obj)


cpdef Py_ssize_t get_itemsize(data):
    '''
    Array and memoryview types have the itemsize attribute while the itemsize for bytes and bytearrays is always 1.
    '''
    try:
        return int(getattr(data, 'itemsize' , 1))
    except TypeError:
        return np.dtype(data).itemsize  # numpy type object, e.g. np.int64


cpdef bint is_array(obj):
    '''
    Check if an object is an array type.

    :param obj: Object to be checked.
    :type obj: object
    :rtype: bool
    '''
    return obj.__class__.__name__ in ARRAY_TYPES


cpdef bint is_array_like(obj):
    '''
    Check if an object is an array type.

    :param obj: Object to be checked.
    :type obj: object
    :rtype: bool
    '''
    return obj.__class__.__name__ in ARRAY_LIKE_TYPES


cpdef bint is_bytes(data):
    '''
    Check if data is bytes (bytes for Python 3, str for Python 2).
    '''
    return data.__class__ == bytes


cpdef bint is_data(data):
    '''
    Whether or not object is data (array-like or bytes).
    '''
    return data.__class__.__name__ in {
        'bytes',
        'bytearray',
        'memoryview',
        '_memoryviewslice',
        'ndarray',
        'MaskedArray',
    }


cpdef bint is_data_iterable(obj):
    '''
    Whether or not object is an iterable of data chunks.
    '''
    return not isinstance(obj, (bytes, str)) and (isinstance(obj, Sequence) or is_generator(obj))


cpdef bint is_generator(obj):
    '''
    Replacement for inspect.isgenerator which works with Cython generators.

    https://bitbucket.org/simpy/simpy/issues/73/
    '''
    return hasattr(obj, 'throw')


cpdef bint is_iterator(obj):
    '''
    Whether or not obj is an iterator, e.g. iter([]). Returns False for iterables, e.g. [].

    From standard library types.py:
    Iterators in Python aren't a matter of type but of protocol.  A large and changing number of builtin types implement
    *some* flavor of iterator.  Don't check the type!  Use hasattr to check for both "__iter__" and "__next__"
    attributes instead.
    '''
    return hasattr(obj, '__next__') and hasattr(obj, '__iter__')


cpdef bint is_memoryview(obj):
    '''
    Check if an object is a memoryview.

    :param obj: Object to be checked.
    :type obj: object
    :rtype: bool
    '''
    return obj.__class__.__name__ in MEMORYVIEW_TYPES


cpdef bint is_split(obj):
    return isinstance(obj, dict)


def _view_bytes(data, dtype):
    return memoryview(data).cast(dtype.char)


def _view_memoryview(memview, dtype):
    if memview.format in {'b', 'B'} or dtype.char in {'b', 'B'}:
        return memview.cast(dtype.char)
    else:
        return np.frombuffer(memview, dtype=dtype)


def _view_memoryviewslice(memview, dtype):
    return _view_memoryview(memoryview(memview), dtype)


def _view_array(array, dtype):
    return array.view(dtype)


cdef dict VIEW_DTYPE_FUNCTIONS = {
    'bytes': _view_bytes,
    'bytearray': _view_bytes,
    'memoryview': _view_memoryview,
    '_memoryviewslice': _view_memoryviewslice,
    'ndarray': _view_array,
    'MaskedArray': _view_array,
}


cpdef view_dtype(data, dtype):
    if dtype is False:
        return data
    elif dtype is None:
        return bytes(data)
    else:
        return VIEW_DTYPE_FUNCTIONS[data.__class__.__name__](data, np.dtype(dtype))
