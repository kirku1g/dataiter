import unittest

from bz2 import decompress
from io import BytesIO

from dataiter import file_writer, split_extension


class TestSplitExtension(unittest.TestCase):  # TODO
    def test_split_extension(self):
        metadata = {'split': 1}
        self.assertEqual(split_extension(metadata), '001')
        metadata['serial_number'] = '123'
        self.assertEqual(split_extension(metadata), '123.001')
        metadata['wps'] = '64'
        self.assertEqual(split_extension(metadata), '123.64.001')
        metadata['frame_double'] = True
        self.assertEqual(split_extension(metadata), '123.64.D.001')
        metadata['frame_double'] = False
        self.assertEqual(split_extension(metadata), '123.64.S.001')
        metadata['split'] = 23
        self.assertEqual(split_extension(metadata), '123.64.S.023')


class TestFileWriter(unittest.TestCase):
    def test_file_writer_buffer(self):
        data = [b'abc']
        buffer = BytesIO()
        file_writer(buffer, data, compression=None)
        self.assertEqual(buffer.getvalue(), data[0])
        buffer = BytesIO()
        file_writer(buffer, data, compression='bz2')
        self.assertEqual(decompress(buffer.getvalue()), data[0])

