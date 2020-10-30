# cython: language_level=3, boundscheck=False
import unittest

import numpy as np

from dataiter cimport buffer as bf


class TestDataBufferUint8(unittest.TestCase):
    def test_data_buffer_uint8(self):
        cdef bf.DataBufferUint8 buff = bf.DataBufferUint8()
        self.assertEqual(buff.size, 0)
        data = buff.read(10)
        self.assertEqual(len(data), 0)
        buff.add(np.zeros(0, dtype=np.uint8))
        self.assertEqual(buff.size, 0)
        buff.add(np.array([5], dtype=np.uint8))
        self.assertEqual(buff.size, 1)
        self.assertEqual(list(buff.peek(1)), [5])
        self.assertEqual(buff.size, 1)
        self.assertEqual(list(buff.read(1)), [5])
        self.assertEqual(buff.size, 0)
        buff.add(np.array([3], dtype=np.uint8))
        buff.add(np.array([8], dtype=np.uint8))
        self.assertEqual(buff.size, 2)
        buff.truncate(0)
        self.assertEqual(buff.size, 2)
        buff.truncate(1)
        self.assertEqual(buff.size, 1)
        buff.truncate(5)
        self.assertEqual(buff.size, 0)
        buff.add(np.arange(0, 7, dtype=np.uint8))
        buff.add(np.arange(20, 23, dtype=np.uint8))
        buff.add(np.arange(30, 36, dtype=np.uint8))
        self.assertEqual(buff.size, 16)
        self.assertEqual(list(buff.peek(2)), [0, 1])
        self.assertEqual(buff.size, 16)
        self.assertEqual(list(buff.read(2)), [0, 1])
        self.assertEqual(buff.size, 14)
        buff.truncate(7)
        self.assertEqual(buff.size, 7)
        self.assertEqual(list(buff.peek(5)), [22, 30, 31, 32, 33])
        self.assertEqual(buff.size, 7)
        self.assertEqual(list(buff.read(5)), [22, 30, 31, 32, 33])
        self.assertEqual(buff.size, 2)


class TestWriteBufferUint8(unittest.TestCase):
    def test_init(self):
        self.assertRaises(ValueError, bf.WriteBufferUint8, -1)
        self.assertRaises(ValueError, bf.WriteBufferUint8, 0)
        cdef bf.WriteBufferUint8 write_buffer = bf.WriteBufferUint8(8)
        self.assertEqual(write_buffer.size, 0)

    def test_write(self):
        cdef bf.WriteBufferUint8 write_buffer = bf.WriteBufferUint8(16)
        write_buffer.write(b'a')
        self.assertEqual(write_buffer.size, 1)
        self.assertEqual(write_buffer.buffer[0], ord(b'a'))
        data = np.arange(15, dtype=np.uint8)
        write_buffer.write(data)
        self.assertEqual(write_buffer.size, 16)
        self.assertEqual(list(write_buffer.buffer), [ord(b'a')] + data.tolist())
        write_buffer = bf.WriteBufferUint8(16)
        data1 = np.array([125.85], dtype=np.float64)
        write_buffer.write(data1)
        self.assertEqual(write_buffer.size, 8)
        self.assertEqual(np.frombuffer(write_buffer.buffer[:8]).tolist(), data1.tolist())
        data2 = np.array([95386.251], dtype=np.float64)
        write_buffer.write(data2)
        self.assertEqual(write_buffer.size, 16)
        self.assertEqual(np.frombuffer(write_buffer.buffer).tolist(), data1.tolist() + data2.tolist())

    def test_write_beyond_size_raises(self):
        cdef bf.WriteBufferUint8 write_buffer = bf.WriteBufferUint8(14)
        with self.assertRaises(ValueError):
            write_buffer.write(np.zeros(2, dtype=np.float64))

    def test_write_uint8(self):
        cdef bf.WriteBufferUint8 write_buffer = bf.WriteBufferUint8(8)
        write_buffer.write_uint8(np.zeros(0, dtype=np.uint8))
        self.assertEqual(write_buffer.size, 0)
        data1 = b'\x24\x36\x48\x5A'
        write_buffer.write_uint8(data1)
        self.assertEqual(write_buffer.size, 4)
        self.assertEqual(bytes(write_buffer.buffer[:4]), data1)
        data2 = np.arange(16, 20, dtype=np.uint8)
        write_buffer.write_uint8(data2)
        self.assertEqual(write_buffer.size, 8)
        self.assertEqual(bytes(write_buffer.buffer), data1 + bytes(data2))

    def test_write_uint8_beyond_size_raises(self):
        cdef bf.WriteBufferUint8 write_buffer = bf.WriteBufferUint8(14)
        with self.assertRaises(ValueError):
            write_buffer.write_uint8(np.zeros(16, dtype=np.uint8))


class TestWriteBufferUint16(unittest.TestCase):
    def test_init(self):
        self.assertRaises(ValueError, bf.WriteBufferUint16, -1)
        self.assertRaises(ValueError, bf.WriteBufferUint16, 0)
        cdef bf.WriteBufferUint16 write_buffer = bf.WriteBufferUint16(8)
        self.assertEqual(write_buffer.size, 0)
        self.assertEqual(len(write_buffer.flush()), 0)
        with self.assertRaises(ValueError):
            write_buffer.write_uint8(np.zeros(1, dtype=np.uint8))
        write_buffer.write_uint8(np.zeros(0, dtype=np.uint8))
        self.assertEqual(write_buffer.size, 0)
        self.assertEqual(list(write_buffer.flush()), [])
        data = np.array([0x24, 0x36, 0x48, 0x5A], dtype=np.uint8)
        write_buffer.write_uint8(data)
        self.assertEqual(write_buffer.size, 2)
        self.assertEqual(list(write_buffer.flush()), [0x3624, 0x5A48])
        self.assertEqual(write_buffer.size, 0)
        write_buffer.write_uint8(data, byteswap=True)
        self.assertEqual(write_buffer.size, 2)
        self.assertEqual(list(write_buffer.flush()), [0x2436, 0x485A])
        self.assertEqual(write_buffer.size, 0)
        data = np.array([0x2436, 0x485A], dtype=np.uint16)
        write_buffer.write_uint16(data)
        self.assertEqual(write_buffer.size, 2)
        write_buffer.write_uint16(data)
        self.assertEqual(write_buffer.size, 4)
        self.assertEqual(list(write_buffer.flush()), [0x2436, 0x485A, 0x2436, 0x485A])
        self.assertEqual(write_buffer.size, 0)
