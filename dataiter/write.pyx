# cython: language_level=3, boundscheck=False
from io import BufferedWriter
import os

import numpy as np

from dataiter.compression import (
    COMPRESSION_CLASSES,
    DEFAULT_COMPRESSION,
    FILE_CLASSES,
    iter_compress,
)
from dataiter cimport types
from dataiter import iterate as it



# Defines a standard postfix order for file metadata.
METADATA_KEYS = ('serial_number', 'wps', 'frame_double', 'split')


def iter_hash(data_iter, hash):
    '''
    Update a hash object with data from a generator.
    '''
    for data in data_iter:
        hash.update(data)
        yield data


def open_writable_file(filepath, compression=DEFAULT_COMPRESSION):
    '''
    :rtype: None
    '''
    if compression:
        filepath += '.' + compression
    return filepath, COMPRESSION_CLASSES[compression](filepath, 'xb')


def decompressed_filepath(filepath):
    '''
    Remove one extension of a compressed file format as it is unexpected
    that a writer would be passed pre-compressed data.
    '''
    name, ext = os.path.splitext(filepath)
    return name if ext.lstrip('.') in FILE_CLASSES else filepath


def write_data(fileobj, data):
    if isinstance(data, np.ndarray) and isinstance(fileobj, BufferedWriter):
        data.tofile(fileobj)
    else:
        fileobj.write(data)


def memory_writer(data_iter):
    '''
    Write data iterable to list.
    '''
    return list(it.iter_data(data_iter))


def file_writer(filelike, data_iter, compression=DEFAULT_COMPRESSION):
    '''
    Write data iterable to filepath or file-like object (ignores splits).
    '''
    if isinstance(filelike, str):
        filepath = decompressed_filepath(filelike)
        filepath, fileobj = open_writable_file(filepath, compression=compression)
        for data in it.iter_data(data_iter):
            write_data(fileobj, data)
        return filepath
    else:
        data_iter = it.iter_as_dtype(it.iter_data(data_iter), None)

        if compression:
            data_iter = iter_compress(data_iter, compression)

        for data in data_iter:
            filelike.write(data)

        return filelike
