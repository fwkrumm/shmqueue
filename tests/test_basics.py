"""
tests of basics (lock/release) of shmlock package
"""
import gc
import pickle
import time
import unittest
from queue import Empty, Full

import msgpack

import shmqueue


# for debug
#logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

class testclass:  # pylint: disable=invalid-name,too-few-public-methods
    """
    simple test class to test put/get methods of shmqueue
    """
    def __init__(self, a):
        self.a = a

    def get_a(self):
        """
        return value of a

        Returns
        -------
        any
            in the tests a is an integer, but this can be any type
        """
        return self.a

class BasicsTest(unittest.TestCase):
    """
    test of basics of shmqueue package

    Parameters
    ----------
    unittest : _type_
        _description_
    """

    def __init__(self, *args, **kwargs):
        """
        test init method
        """
        super().__init__(*args, **kwargs)

    def test_init(self):
        """
        test initialization of shmqueue
        """
        name = str(time.time())

        s = shmqueue.ShmQueue(name)
        # check queue data
        self.assertEqual(s.identifier, name)
        self.assertEqual(s.qsize(), 0)
        self.assertEqual(s.refs, 1)
        # check ref counter
        s2 = shmqueue.ShmQueue(name)
        s3 = shmqueue.ShmQueue(name)
        self.assertEqual(s.refs, 3)
        # s2.shutdown() # test with console handlers?
        # s3.shutdown() # test with console handlers?
        del s2
        del s3
        gc.collect()  # force garbage collection to release shared memory
        time.sleep(1)  # give some time for the garbage collector to finish
        self.assertEqual(s.refs, 1)

    def test_buffer_size_handling(self):
        """
        check buffer sizes are handled correctly
        """
        # default size should be system page size
        name = str(time.time())
        s = shmqueue.ShmQueue(name)
        self.assertEqual(s.max_buffer_size(), shmqueue.SYSTEM_PAGESIZE)
        # check that a buffer which is smaller than system page size is handled correctly
        d = shmqueue.ShmQueue(name, buffer_size=1)
        self.assertEqual(d.max_buffer_size(), shmqueue.SYSTEM_PAGESIZE)
        with self.assertRaises(shmqueue.exceptions.ShmQueueValueError):
            shmqueue.ShmQueue(name, buffer_size=0)
        with self.assertRaises(shmqueue.exceptions.ShmQueueTypeError):
            shmqueue.ShmQueue(name, buffer_size="123")
        with self.assertRaises(shmqueue.exceptions.ShmQueueValueError):
            shmqueue.ShmQueue(name, buffer_size=-1)
        with self.assertRaises(shmqueue.exceptions.ShmQueueTypeError):
            # no string name
            shmqueue.ShmQueue(123)

    def test_buffer_occupation(self):
        """
        check buffer occupation is handled correctly; we assume default picke module for now
        """
        name = str(time.time())
        s = shmqueue.ShmQueue(name, buffer_size=shmqueue.SYSTEM_PAGESIZE)
        self.assertEqual(s.buffer_occupancy(), 0)

        # put some data to the queue and check buffer occupancy
        data = None
        serialized_data = msgpack.dumps(data)
        s.put(data)
        self.assertEqual(s.buffer_occupancy(), len(serialized_data) + shmqueue.SIZE_HEADER)

        # more data; since msgpack cannot serialize the testclass, pickle should be used as fallback
        old_occupancy = s.buffer_occupancy()
        data = testclass(123)
        pickled_data = pickle.dumps(data)
        s.put(data)
        self.assertEqual(s.buffer_occupancy(), old_occupancy + \
            shmqueue.SIZE_HEADER + len(pickled_data))

        # now check if we have some ring buffer overflow
        for _ in range(s.qsize()):
            _ = s.get()
        # queue should be empty now
        self.assertEqual(s.buffer_occupancy(), 0)
        self.assertTrue(s.empty())

        # does the following work for all systems or might there be system pagesizes which might
        # cause issues?

        # find a data size which leads to an "overflow" of the ring buffer i.e. an occupation
        # of the already processed buffer
        data = []
        required_length = len(data)
        while required_length < shmqueue.SYSTEM_PAGESIZE:
            data.append(testclass(len(data)))
            required_length = len(pickle.dumps(data)) + shmqueue.SIZE_HEADER
        # pop last element so that it fits into the buffer again
        data.pop(-1)
        required_length = len(pickle.dumps(data)) + shmqueue.SIZE_HEADER

        # now put data to the queue and check that the ring buffer worked i.e. that the data
        # split over the buffer
        s.put(data)
        self.assertTrue(s.debug_ref_data.get_pos > s.debug_ref_data.put_pos)

        result = s.get()

        for idx, item in enumerate(result):
            # check that test class is deserialized correctly
            self.assertIsInstance(item, testclass, f"item {idx} should be of type testclass")
            self.assertEqual(data[idx].get_a(), item.get_a())

    def test_put_get(self):
        """
        check get and put methods
        """
        name = str(time.time())
        s = shmqueue.ShmQueue(name)
        self.assertEqual(s.refs, 1)
        s.put(None)
        self.assertFalse(s.empty())
        self.assertEqual(s.qsize(), 1)
        self.assertEqual(s.get(), None)
        self.assertEqual(s.qsize(), 0)
        # test classes as objects and if default serialization (pickle) works
        s.put(testclass(123))
        s.put(testclass(456))
        self.assertEqual(s.qsize(), 2)
        self.assertEqual(s.get().get_a(), 123)
        self.assertEqual(s.get().get_a(), 456)
        self.assertEqual(s.qsize(), 0)

        with self.assertRaises(shmqueue.exceptions.ShmQueueEmpty):
            s.get(block=False)

        with self.assertRaises(Empty):
            # also supports standard queue api
            s.get(block=False)


        with self.assertRaises(shmqueue.exceptions.ShmQueueValueError):
            # can never fit into the buffer
            s.put(list(range(shmqueue.SYSTEM_PAGESIZE)))

        def fill_until_full():
            """
            fill the queue until it is full
            """
            while True:
                s.put(testclass(123), block=False)

        with self.assertRaises(shmqueue.exceptions.ShmQueueFull):
            # fill the queue until it is full and check that assertion is raised
            fill_until_full()

        with self.assertRaises(Full):
            # also supports standard queue api
            s.put(testclass(123), block=False)

    def test_clear(self):
        """
        test clear method
        """
        name = str(time.time())
        s = shmqueue.ShmQueue(name)
        self.assertEqual(s.qsize(), 0)
        s.put(testclass(123))
        s.put(testclass(456))
        self.assertEqual(s.qsize(), 2)
        s.clear()
        self.assertEqual(s.qsize(), 0)


    def test_shutdown(self):
        """
        test shutdown method
        """
        name = str(time.time())
        s = shmqueue.ShmQueue(name)
        self.assertEqual(s.refs, 1)
        s.shutdown()

        with self.assertRaises(shmqueue.exceptions.ShmQueueNotInitializedError):
            s.qsize()

        with self.assertRaises(shmqueue.exceptions.ShmQueueNotInitializedError):
            _ = s.refs

    def test_more_types(self):
        """
        test more types of data
        """
        name = str(time.time())
        s = shmqueue.ShmQueue(name)

        # test string
        s.put("test")
        self.assertEqual(s.get(), "test")

        # test bytes
        s.put(b"test")
        self.assertEqual(s.get(), b"test")

        # test integer
        s.put(123)
        self.assertEqual(s.get(), 123)

        # test float
        s.put(123.456)
        self.assertEqual(s.get(), 123.456)

        # test list
        s.put([1, 2, 3])
        self.assertEqual(s.get(), [1, 2, 3])

        # test dict
        s.put({"a": 1, "b": 2})
        self.assertEqual(s.get(), {"a": 1, "b": 2})

        # test tuple
        s.put((1, 2, 3))
        self.assertEqual(s.get(), [1, 2, 3])  # tuples are converted to lists

        # test set
        s.put({1, 2, 3})
        self.assertEqual(s.get(), {1, 2, 3})

        # test None
        s.put(None)
        self.assertEqual(s.get(), None)

        # test boolean
        s.put(True)
        self.assertEqual(s.get(), True)
        s.put(False)
        self.assertEqual(s.get(), False)


class FullTest(unittest.TestCase):
    """
    Tests for ShmQueue.full().
    This test was AI generated but check for correctness and completeness.

    Strategy: use items whose in-buffer footprint (SIZE_HEADER + msgpack payload) divides
    SYSTEM_PAGESIZE exactly so the buffer can be filled byte-for-byte.

    Item: b'\\x00' * 6  →  msgpack bin8: 0xc4 0x06 + 6 bytes = 8 bytes payload
    In-buffer size: SIZE_HEADER(8) + 8 = 16 bytes
    Count to fill one page: SYSTEM_PAGESIZE // 16  (always integer since
    page size is power-of-2 ≥ 4096)
    """

    # item whose msgpack serialization is exactly 8 bytes (bin8 format)
    _ITEM = b"\x00" * 6
    _ITEM_SIZE = shmqueue.SIZE_HEADER + len(msgpack.dumps(_ITEM))  # must be 16
    assert shmqueue.SYSTEM_PAGESIZE % _ITEM_SIZE == 0, \
        "SYSTEM_PAGESIZE must be divisible by _ITEM_SIZE for the full() test to be exact"
    _ITEMS_TO_FILL = shmqueue.SYSTEM_PAGESIZE // _ITEM_SIZE

    def _make_queue(self) -> shmqueue.ShmQueue:
        return shmqueue.ShmQueue(str(time.time()), buffer_size=shmqueue.SYSTEM_PAGESIZE)

    def test_full_returns_false_when_empty(self):
        s = self._make_queue()
        self.assertFalse(s.full())

    def test_full_returns_false_when_partially_filled(self):
        s = self._make_queue()
        # put half the items needed to fill the buffer
        for _ in range(self._ITEMS_TO_FILL // 2):
            s.put(self._ITEM)
        self.assertFalse(s.full())
        self.assertFalse(s.empty())

    def test_full_returns_true_when_buffer_exactly_full(self):
        s = self._make_queue()
        # fill the buffer byte-for-byte: put_pos wraps to 0, same_lap flips to 0
        for _ in range(self._ITEMS_TO_FILL):
            s.put(self._ITEM)
        self.assertTrue(s.full())
        # sanity: empty must be False when full
        self.assertFalse(s.empty())

    def test_full_returns_false_after_one_get(self):
        s = self._make_queue()
        for _ in range(self._ITEMS_TO_FILL):
            s.put(self._ITEM)
        self.assertTrue(s.full())
        s.get()
        self.assertFalse(s.full())

    def test_full_and_empty_are_mutually_exclusive(self):
        """A queue cannot be both full and empty at the same time."""
        s = self._make_queue()
        # empty state
        self.assertFalse(s.full() and s.empty())
        # full state
        for _ in range(self._ITEMS_TO_FILL):
            s.put(self._ITEM)
        self.assertFalse(s.full() and s.empty())

    def test_full_false_after_clear(self):
        s = self._make_queue()
        for _ in range(self._ITEMS_TO_FILL):
            s.put(self._ITEM)
        self.assertTrue(s.full())
        s.clear()
        self.assertFalse(s.full())
        self.assertTrue(s.empty())


if __name__ == "__main__":
    unittest.main(verbosity=2)
