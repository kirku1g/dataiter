# cython: language_level=3, boundscheck=False
import numpy as np
import unittest

from dataiter import types


class TestAsDtype(unittest.TestCase):
    def test_as_dtype(self):
        data = b'abc'
        self.assertEqual(types.as_dtype(data, None), data)
        dtype = np.uint8
        self.assertTrue((types.as_dtype(data, dtype) == np.frombuffer(data, dtype=np.uint8)).all())
        data = np.arange(3)
        self.assertEqual(types.as_dtype(data, None), data.tostring())
        self.assertTrue((types.as_dtype(data, dtype) == data.astype(dtype)).all())


class TestAsInt(unittest.TestCase):
    def test_as_int(self):
        self.assertEqual(types.as_int(1), 1)
        self.assertEqual(types.as_int(1.5), 1)
        self.assertEqual(types.as_int([10]), 10)
        self.assertEqual(types.as_int((64,)), 64)
        self.assertRaises(ValueError, types.as_int, [])
        self.assertRaises(ValueError, types.as_int, [10, 20])
        self.assertRaises(ValueError, types.as_int, (10, 20, 30))


class TestAsSequence(unittest.TestCase):
    def test_as_sequence(self):
        self.assertEqual(types.as_sequence(None), None)
        self.assertEqual(types.as_sequence(3), (3,))
        self.assertEqual(types.as_sequence(2.4), (2.4,))
        self.assertEqual(types.as_sequence([]), [])
        self.assertEqual(types.as_sequence([5]), [5])
        self.assertEqual(types.as_sequence([7, 9]), [7, 9])
        self.assertEqual(types.as_sequence(tuple()), tuple())
        self.assertEqual(types.as_sequence((1,)), (1,))
        self.assertEqual(types.as_sequence((1, 2, 3)), (1, 2, 3))
        self.assertEqual(types.as_sequence('abc', ignore={str}), ('abc',))
        self.assertEqual(types.as_sequence(b'abc', ignore={bytes}), (b'abc',))
        self.assertEqual(types.as_sequence(['abc'], ignore={list}), (['abc'],))
        self.assertEqual(types.as_sequence('abc', ignore={bytes}), 'abc')


class TestIsArray(unittest.TestCase):
    def test_is_array(self):
        self.assertTrue(types.is_array(np.arange(10)))
        self.assertFalse(types.is_array({}))


class TestIsData(unittest.TestCase):
    def test_is_data(self):
        # TODO: memoryview
        self.assertTrue(types.is_data(np.arange(10)))
        self.assertTrue(types.is_data(b'abc'))
        self.assertFalse(types.is_data('abc'))
        self.assertFalse(types.is_data(7))
        self.assertFalse(types.is_data(None))


class TestIsDataIterable(unittest.TestCase):
    def test_is_data(self):
        # TODO: memoryview
        self.assertFalse(types.is_data_iterable(np.arange(10)))
        self.assertFalse(types.is_data_iterable(b'abc'))
        self.assertFalse(types.is_data_iterable('abc'))
        self.assertFalse(types.is_data_iterable(7))
        self.assertFalse(types.is_data_iterable(None))
        self.assertTrue(types.is_data_iterable([b'abc']))
        self.assertTrue(types.is_data_iterable([np.arange(10)]))
        self.assertTrue(types.is_data_iterable(tuple(np.arange(10),)))
        self.assertTrue(types.is_data_iterable(d for d in [b'abc']))
