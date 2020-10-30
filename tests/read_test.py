import os
import unittest

import numpy as np

from dataiter import iterate as it, read, types


PACKAGE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEST_DATA_PATH = os.path.join(os.path.dirname(PACKAGE_PATH), 'tests', 'test_data')


class ReaderImplementationTest:
    def setUp(self):
        self.filepath = os.path.join(TEST_DATA_PATH, 'flight_data', 'l3uqar', '01', 'serial_num_7_digits.dat')  # TODO
        with open(self.filepath, 'rb') as f:
            self.data = f.read()

    def test_reader(self):
        with self.cls(self.input) as data_iter:
            self.assertEqual([bytes(d) for d in data_iter], [self.data])
        with self.cls(self.input, dtype=np.uint8) as data_iter:
            result = list(data_iter)
            self.assertEqual(len(result), 1)
            self.assertEqual(types.get_dtype(result[0]), np.uint8)
            self.assertEqual(bytes(result[0]), self.data)
        with self.cls(self.input, count=0x4000) as data_iter:
            self.assertEqual([bytes(d) for d in data_iter], [self.data[:0x4000], self.data[0x4000:]])
        with self.cls(self.input, count=0x2000) as data_iter:
            self.assertEqual([bytes(d) for d in data_iter], [self.data[x*0x2000:(x+1)*0x2000] for x in range(4)])
        with self.cls(self.input, stop=0x2000) as data_iter:
            self.assertEqual([bytes(d) for d in data_iter], [self.data[:0x2000]])
        with self.cls(self.input, start=0x6000) as data_iter:
            self.assertEqual([bytes(d) for d in data_iter], [self.data[0x6000:]])
        with self.cls(self.input, start=0x2000, stop=0x4000) as data_iter:
            self.assertEqual([bytes(d) for d in data_iter], [self.data[0x2000:0x4000]])
        with self.cls(self.input, start=0x2000, stop=0x4000, count=0x1000) as data_iter:
            self.assertEqual([bytes(d) for d in data_iter], [self.data[0x2000:0x3000], self.data[0x3000:0x4000]])
        with self.cls(self.input, start=0x2500, stop=0x4000, count=0x1000) as data_iter:
            self.assertEqual([bytes(d) for d in data_iter], [self.data[0x2500:0x3500], self.data[0x3500:0x4000]])

    def test_all(self):
        self.assertEqual(bytes(self.cls(self.input).all()), self.data)
        self.assertEqual(bytes(self.cls(self.input, count=0x2000).all()), self.data)
        self.assertEqual(bytes(self.cls(self.input, count=1).all()), self.data)
        result = self.cls(self.input, dtype=np.uint8).all()
        self.assertEqual(types.get_dtype(result), np.uint8)
        self.assertEqual(bytes(result), self.data)
        self.assertEqual(bytes(self.cls(self.input, start=0x2000).all()), self.data[0x2000:])
        self.assertEqual(bytes(self.cls(self.input, stop=0x2000).all()), self.data[:0x2000])
        self.assertEqual(bytes(self.cls(self.input, start=0x2000, stop=0x4000).all()), self.data[0x2000:0x4000])
        result = self.cls(self.input, dtype=np.uint32, start=0x2000, stop=0x4000).all()
        self.assertEqual(types.get_dtype(result), np.uint32)
        self.assertEqual(bytes(result), self.data[0x2000:0x4000])
        result = self.cls(self.input, dtype=np.int32, start=2048, stop=4096, byte=False).all()
        self.assertEqual(types.get_dtype(result), np.int32)
        self.assertEqual(bytes(result), self.data[0x2000:0x4000])

    def test_first(self):
        self.assertEqual(bytes(self.cls(self.input).first()), self.data)
        result = self.cls(self.input, dtype=np.uint16).first()
        self.assertEqual(types.get_dtype(result), np.uint16)
        self.assertEqual(bytes(result), self.data)
        self.assertEqual(bytes(self.cls(self.input, count=0x2000).first()), self.data[:0x2000])
        self.assertEqual(bytes(self.cls(self.input, stop=0x1000).first()), self.data[:0x1000])
        self.assertEqual(bytes(self.cls(self.input, start=0x6000).first()), self.data[0x6000:])
        self.assertEqual(bytes(self.cls(self.input, start=0x3000, stop=0x4000).first()), self.data[0x3000:0x4000])


class TestFileReader(ReaderImplementationTest, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cls = read.file_reader
        self.input = self.filepath


class TestDataReaderBytes(ReaderImplementationTest, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cls = read.data_reader
        self.input = self.data


class TestDataReaderArray(ReaderImplementationTest, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cls = read.data_reader
        self.input = np.frombuffer(self.data)

    def test_reader(self):
        with self.cls(self.input) as data_iter:
            reader_data = list(data_iter)
            self.assertEqual(len(reader_data), 1)
            self.assertTrue((reader_data[0] == self.input).all())
        with self.cls(self.input, dtype=np.uint8) as data_iter:
            reader_data = list(data_iter)
            self.assertEqual(len(reader_data), 1)
            self.assertTrue((reader_data[0] == self.input.view(np.uint8)).all())
        with self.cls(self.input, count=2048) as data_iter:
            reader_data = list(data_iter)
            self.assertEqual(len(reader_data), 2)
            self.assertTrue((reader_data[0] == self.input[:2048]).all())
            self.assertTrue((reader_data[1] == self.input[2048:]).all())
        with self.cls(self.input, stop=1024) as data_iter:
            reader_data = list(data_iter)
            self.assertEqual(len(reader_data), 1)
            self.assertTrue((reader_data[0] == self.input[:1024]).all())
        with self.cls(self.input, start=3096) as data_iter:
            reader_data = list(data_iter)
            self.assertEqual(len(reader_data), 1)
            self.assertTrue((reader_data[0] == self.input[3096:]).all())
        with self.cls(self.input, count=128, start=512, stop=704) as data_iter:
            reader_data = list(data_iter)
            self.assertEqual(len(reader_data), 2)
            self.assertTrue((reader_data[0] == self.input[512:640]).all())
            self.assertTrue((reader_data[1] == self.input[640:704]).all())
        with self.cls(self.input, dtype=np.uint32, count=32, start=512, stop=704) as data_iter:
            reader_data = list(data_iter)
            self.assertEqual(len(reader_data), 2)
            self.assertTrue((reader_data[0] == self.input.view(np.uint32)[128:160]).all())
            self.assertTrue((reader_data[1] == self.input.view(np.uint32)[160:176]).all())
        with self.cls(self.input, dtype=np.uint32, count=32, start=128, stop=176, byte=False) as data_iter:
            reader_data = list(data_iter)
            self.assertEqual(len(reader_data), 2)
            self.assertTrue((reader_data[0] == self.input.view(np.uint32)[128:160]).all())
            self.assertTrue((reader_data[1] == self.input.view(np.uint32)[160:176]).all())

    def test_all(self):
        self.assertTrue((self.cls(self.input).all() == self.input).all())
        self.assertTrue((self.cls(self.input, count=512).all() == self.input).all())
        self.assertTrue((self.cls(self.input, dtype=np.float64).all() == self.input).all())
        self.assertTrue((self.cls(self.input, dtype=np.uint32).all() == self.input.view(np.uint32)).all())
        self.assertTrue((self.cls(self.input, start=128).all() == self.input[128:]).all())
        self.assertTrue((self.cls(self.input, start=128, stop=256).all() == self.input[128:256]).all())
        self.assertTrue((self.cls(self.input, count=512, start=128, stop=256).all() == self.input[128:256]).all())
        self.assertTrue((self.cls(self.input, count=64, start=128, stop=256).all() == self.input[128:256]).all())
        self.assertTrue((self.cls(self.input, dtype=np.uint32, count=64, start=128, stop=256).all() ==
                         self.input.view(dtype=np.uint32)[32:64]).all())
        self.assertTrue((self.cls(self.input, dtype=np.uint32, count=64, start=128, stop=256, byte=False).all() ==
                         self.input.view(dtype=np.uint32)[128:256]).all())

    def test_first(self):
        self.assertTrue((self.cls(self.input).first() == self.input).all())
        self.assertTrue((self.cls(self.input, dtype=np.uint8).first() == self.input.view(np.uint8)).all())
        self.assertTrue((self.cls(self.input, count=128).first() == self.input[:128]).all())
        self.assertTrue((self.cls(self.input, stop=256).first() == self.input[:256]).all())
        self.assertTrue((self.cls(self.input, start=256).first() == self.input[256:]).all())
        self.assertTrue((self.cls(self.input, start=384, stop=512).first() == self.input[384:512]).all())
        self.assertTrue((self.cls(self.input, count=64, start=384, stop=512).first() == self.input[384:448]).all())
        self.assertTrue((self.cls(self.input, dtype=np.uint32, count=64, start=384, stop=1024).first() ==
                         self.input.view(np.uint32)[96:160]).all())
        self.assertTrue((self.cls(self.input, dtype=np.uint32, count=64, start=384, stop=512).first() ==
                         self.input.view(np.uint32)[96:128]).all())
        self.assertTrue((self.cls(self.input, dtype=np.uint32, count=64, start=384, stop=1024, byte=False).first() ==
                         self.input.view(np.uint32)[384:448]).all())


class TestIterableReader(ReaderImplementationTest, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.cls = read.iterable_reader

    @property
    def input(self):
        return it.chunk_uint8([self.data], 4096, flush=True)


class TestReader(unittest.TestCase):
    def test_reader_path(self):
        path = 'path'
        r = read.reader(path)
        self.assertTrue(isinstance(r, read.file_reader))
        self.assertEqual(r.name, path)

    def test_reader_bytes(self):
        data = b'data'
        r = read.reader(data)
        self.assertTrue(isinstance(r, read.data_reader))
        self.assertEqual(list(r), [data])

    def test_reader_array(self):
        array = np.arange(1)
        r = read.reader(array)
        self.assertTrue(isinstance(r, read.data_reader))
        self.assertEqual(list(r), [array])

    def test_reader_iterable(self):
        data_iter = (x for x in [b'abc'])
        r = read.reader(data_iter)
        self.assertTrue(isinstance(r, read.iterable_reader))
        self.assertEqual([bytes(x) for x in r], [b'abc'])

    #def test_reader_reader(self):
        #data = b'data'
        #reader_input = read.reader(data)
        #reader_output = read.reader(reader_input)
        #self.assertEqual(reader_input.data, reader_output.data)
        #stop = 10
        #reader_output = read.reader(reader_input, stop=stop)
        #self.assertEqual(reader_output.stop, stop)


class TestReadSize(unittest.TestCase):
    def test_read_size(self):
        # READ_SIZE is assumed to be a multiple of megabytes in various reader use cases for brevity
        self.assertGreaterEqual(read.READ_SIZE, 1024 * 1024)
        self.assertEqual(read.READ_SIZE % (1024 * 1024), 0)


if __name__ == '__main__':
    unittest.main()
