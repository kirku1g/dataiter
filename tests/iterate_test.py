import numpy as np
import unittest

from dataiter import iterate as it, types


class TestChunk(unittest.TestCase):
    def test_chunk_bytes(self):
        data = [b'123', b'45', b'6', b'', b'789']
        self.assertRaises(ValueError, list, it.chunk(iter(data), 0))
        self.assertEqual([bytes(c) for c in it.chunk(iter(data), 1)],
                         [b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9'])
        self.assertEqual([bytes(c) for c in it.chunk(iter(data), 2)], [b'12', b'34', b'56', b'78'])
        self.assertEqual([bytes(c) for c in it.chunk(iter(data), 2, flush=True)], [b'12', b'34', b'56', b'78', b'9'])
        self.assertEqual([bytes(c) for c in it.chunk(iter(data), 3)],  [b'123', b'456', b'789'])
        self.assertEqual([bytes(c) for c in it.chunk(iter(data), 3, flush=True)],  [b'123', b'456', b'789'])
        self.assertEqual(list(it.chunk(iter(data), 20)), [])
        self.assertEqual([bytes(c) for c in it.chunk(iter(data), 20, flush=True)], [b''.join(data)])

    def test_chunk_array(self):
        data = [np.array([1,2,3]), np.array([4,5]), np.array([6]), np.array([]), np.array([7,8,9])]
        self.assertEqual(list(x.tolist() for x in it.chunk(iter(data), 3)), [[1,2,3], [4,5,6], [7,8,9]])
        self.assertEqual(list(x.tolist() for x in it.chunk(iter(data), 20)), [])
        self.assertEqual(list(x.tolist() for x in it.chunk(iter(data), 20, flush=True)),
                         [np.concatenate(data).tolist()])


class TestChunkDtype(unittest.TestCase):
    def test_chunk_dtype_bytes(self):
        data = [b'123', b'45', b'6', b'', b'789']
        self.assertRaises(ValueError, list, it.chunk_dtype(iter(data), 0))
        self.assertEqual([bytes(c) for c in it.chunk_dtype(iter(data), 1)],
                         [b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9'])
        self.assertEqual([bytes(c) for c in it.chunk_dtype(iter(data), 2)],
                         [b'12', b'34', b'56', b'78'])
        self.assertEqual([bytes(c) for c in it.chunk_dtype(iter(data), 2, flush=True)],
                         [b'12', b'34', b'56', b'78', b'9'])
        self.assertEqual([bytes(c) for c in it.chunk_dtype(iter(data), 3)],  [b'123', b'456', b'789'])
        self.assertEqual([bytes(c) for c in it.chunk_dtype(iter(data), 3, flush=True)],  [b'123', b'456', b'789'])
        self.assertEqual([bytes(c) for c in it.chunk_dtype(iter(data), 20)], [])
        self.assertEqual([bytes(c) for c in it.chunk_dtype(iter(data), 20, flush=True)], [b''.join(data)])

    def test_chunk_dtype_array(self):
        data = [np.array([1,2,3]), np.array([4,5]), np.array([6]), np.array([]), np.array([7,8,9])]
        self.assertEqual(list(x.tolist() for x in it.chunk_dtype(iter(data), 3, dtype=np.int64)),
                         [[1,2,3], [4,5,6], [7,8,9]])
        self.assertEqual(list(x.tolist() for x in it.chunk_dtype(iter(data), 20, dtype=np.int64)), [])
        self.assertEqual(list(x.tolist() for x in it.chunk_dtype(iter(data), 20, dtype=np.int64, flush=True)),
                         [np.concatenate(data).tolist()])


class TestChunkUint8(unittest.TestCase):
    def test_chunk_uint8(self):
        data = [b'123', b'45', b'6', b'', b'789']
        self.assertRaises(ValueError, list, it.chunk_uint8(iter(data), 0))
        self.assertEqual([bytes(c) for c in it.chunk_uint8(iter(data), 1)],
                         [b'1', b'2', b'3', b'4', b'5', b'6', b'7', b'8', b'9'])
        self.assertEqual([bytes(c) for c in it.chunk_uint8(iter(data), 2)], [b'12', b'34', b'56', b'78'])
        self.assertEqual([bytes(c) for c in it.chunk_uint8(iter(data), 2, flush=True)],
                         [b'12', b'34', b'56', b'78', b'9'])
        self.assertEqual([bytes(c) for c in it.chunk_uint8(iter(data), 3)],  [b'123', b'456', b'789'])
        self.assertEqual([bytes(c) for c in it.chunk_uint8(iter(data), 3, flush=True)],  [b'123', b'456', b'789'])
        self.assertEqual(list(it.chunk_uint8(iter(data), 20)), [])
        self.assertEqual([bytes(c) for c in it.chunk_uint8(iter(data), 20, flush=True)], [b''.join(data)])


class TestJoin(unittest.TestCase):
    def test_join(self):
        self.assertEqual(it.join([]), b'')
        joined = it.join([], dtype=np.uint8)
        self.assertEqual(joined.tolist(), [])
        self.assertEqual(joined.dtype, np.uint8)
        self.assertEqual(it.join([b'abc', b'def']), b'abcdef')
        self.assertEqual(it.join([np.array([1, 2]), np.array([3, 4])]).tolist(), [1, 2, 3, 4])
        joined = it.join([np.array([1, 2]), np.array([3, 4])])
        self.assertEqual(joined.tolist(), [1, 2, 3, 4])
        self.assertEqual(joined.dtype, np.int64)


class TestIterAsDtype(unittest.TestCase):
    def test_iter_as_dtype(self):
        for iterable in [[], [b'abc'], [b'a', b'b', b'c']]:
            self.assertEqual(list(it.iter_as_dtype(iterable, None)), iterable)
        array = np.arange(3)
        self.assertEqual(list(it.iter_as_dtype([array], None)), [array.tostring()])
        dtype = np.uint8
        result = list(it.iter_as_dtype([array], dtype))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].dtype, dtype)
        self.assertEqual(bytes(result[0]), array.astype(np.uint8).tostring())
        data = b'abc'
        result = list(it.iter_as_dtype([data], dtype))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].dtype, dtype)
        self.assertEqual(bytes(result[0]), data)


class TestIterData(unittest.TestCase):
    def test_iter_data(self):
        data_gen = []
        self.assertEqual(list(it.iter_data(data_gen)), data_gen)
        data_gen = [b'abc']
        self.assertEqual(list(it.iter_data(data_gen)), data_gen)
        data_gen = [b'a', b'', b'c']
        self.assertEqual(list(it.iter_data(data_gen)), data_gen)
        data_gen = [b'a', None, b'c']
        self.assertEqual(list(it.iter_data(data_gen)), [b'a', b'c'])
        array1 = np.arange(4)
        array2 = np.arange(4, 8)
        data_gen = [array1, None, array2]
        self.assertEqual(list(it.iter_data(data_gen)), [array1, array2])


class TestIterDataSlice(unittest.TestCase):
    def test_iter_data_slice(self):
        self.assertEqual(list(it.iter_data_slice([], slice(0, 10))), [])
        self.assertEqual(list(it.iter_data_slice([b'abc'], slice(1, 2))), [b'b'])
        data = [b'abc', b'def']
        self.assertEqual(list(it.iter_data_slice(data, slice(1, None))), [b'bc', b'ef'])
        self.assertEqual(list(it.iter_data_slice(data, slice(None, None))), data)
        self.assertEqual(list(it.iter_data_slice(data, slice(0, 3))), data)
        self.assertEqual(list(it.iter_data_slice(data, slice(0, 10))), data)


class TestIterDataSlice(unittest.TestCase):
    def test_iter_data_slice(self):
        self.assertEqual(list(it.iter_data_slices([], [slice(0, 10)])), [])
        self.assertEqual(list(it.iter_data_slices([b'abc'], [slice(1, 2)])), [[b'b']])
        data = [b'abc', b'def']
        self.assertEqual(list(it.iter_data_slices(data, [slice(1, 2), slice(1, None)])), [[b'b', b'bc'], [b'e', b'ef']])
        self.assertEqual(list(it.iter_data_slices(data, [])), [[], []])


class TestIterDataStartIdx(unittest.TestCase):
    def test_iter_data_start_idx(self):
        data = []
        self.assertEqual(list(it.iter_data_start_idx(iter(data), 0)), data)
        self.assertEqual(list(it.iter_data_start_idx(iter(data), 5)), data)
        data = [b'a']
        self.assertEqual(list(it.iter_data_start_idx(iter(data), 0)), data)
        self.assertEqual(list(it.iter_data_start_idx(iter(data), 1)), [])
        data = [b'abc']
        self.assertEqual(list(it.iter_data_start_idx(iter(data), 0)), data)
        self.assertEqual([bytes(d) for d in it.iter_data_start_idx(iter(data), 1)], [b'bc'])
        self.assertEqual([bytes(d) for d in it.iter_data_start_idx(iter(data), 2)], [b'c'])
        self.assertEqual(list(it.iter_data_start_idx(iter(data), 3)), [])
        data = [b'a', b'b', b'c']
        self.assertEqual(list(it.iter_data_start_idx(iter(data), 0)), data)
        self.assertEqual(list(it.iter_data_start_idx(iter(data), 1)), [b'b', b'c'])
        self.assertEqual(list(it.iter_data_start_idx(iter(data), 2)), [b'c'])
        self.assertEqual(list(it.iter_data_start_idx(iter(data), 3)), [])
        data = [np.array([1, 2, 3]), np.array([4, 5])]
        self.assertEqual(list(it.iter_data_start_idx(iter(data), 0)), data)
        self.assertEqual(list(x.tolist() for x in it.iter_data_start_idx(iter(data), 1)), [[2, 3, 4], [5]])
        self.assertEqual(list(x.tolist() for x in it.iter_data_start_idx(iter(data), 2)), [[3, 4, 5]])
        self.assertEqual(list(x.tolist() for x in it.iter_data_start_idx(iter(data), 3)), [[4, 5]])
        self.assertEqual(list(x.tolist() for x in it.iter_data_start_idx(iter(data), 4)), [[5]])
        self.assertEqual(list(it.iter_data_start_idx(iter(data), 5)), [])


class TestIterDataStopIdx(unittest.TestCase):
    def test_iter_data_stop_idx(self):
        data = []
        self.assertEqual(list(it.iter_data_stop_idx((d for d in data), 0)), data)
        self.assertEqual(list(it.iter_data_stop_idx((d for d in data), 5)), data)
        data = [b'a']
        self.assertEqual(list(it.iter_data_stop_idx((d for d in data), 0)), [])
        self.assertEqual(list(it.iter_data_stop_idx((d for d in data), 1)), data)
        data = [b'abc']
        self.assertEqual(list(it.iter_data_stop_idx((d for d in data), 0)), [])
        self.assertEqual(list(it.iter_data_stop_idx((d for d in data), 1)), [b'a'])
        self.assertEqual(list(it.iter_data_stop_idx((d for d in data), 2)), [b'ab'])
        self.assertEqual(list(it.iter_data_stop_idx((d for d in data), 3)), data)
        data = [b'a', b'b', b'c']
        self.assertEqual(list(it.iter_data_stop_idx((d for d in data), 0)), [])
        self.assertEqual(list(it.iter_data_stop_idx((d for d in data), 1)), [b'a'])
        self.assertEqual(list(it.iter_data_stop_idx((d for d in data), 2)), [b'a', b'b'])
        self.assertEqual(list(it.iter_data_stop_idx((d for d in data), 3)), [b'a', b'b', b'c'])


class TestIterViewDtype(unittest.TestCase):
    def test_iter_as_dtype(self):
        for iterable in [[], [b'abc'], [b'a', b'b', b'c']]:
            self.assertEqual(list(it.iter_view_dtype(iterable, None)), iterable)
        array = np.arange(3)
        self.assertEqual(list(it.iter_view_dtype([array], None)), [array.tostring()])
        dtype = np.uint8
        result = list(it.iter_view_dtype([array], dtype))
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].dtype, dtype)
        self.assertEqual(bytes(result[0]), array.tostring())
        data = b'abc'
        result = list(it.iter_view_dtype([data], dtype))
        self.assertEqual(len(result), 1)
        self.assertEqual(types.get_dtype(result[0]), dtype)
        self.assertEqual(bytes(result[0]), data)
