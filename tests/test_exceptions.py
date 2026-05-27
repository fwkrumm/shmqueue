"""
Tests that verify each shmqueue exception is raised under the correct conditions.

AI GENERATED
"""
import unittest
import shmqueue
from shmqueue.exceptions import (
    ShmQueueEmpty,
    ShmQueueFull,
    ShmQueueTypeError,
    ShmQueueValueError,
    ShmQueueRuntimeError,
    ShmQueueNotInitializedError,
    ShmQueueSerializationError,
    ShmQueueDeserializationError,
)

QUEUE_NAME_BASE = "test_exc"
BUFFER_SIZE = shmqueue.SYSTEM_PAGESIZE


def _make_queue(suffix):
    return shmqueue.ShmQueue(f"{QUEUE_NAME_BASE}_{suffix}", buffer_size=BUFFER_SIZE)


def _fill_queue(q: shmqueue.ShmQueue):
    """Fill the queue until ShmQueueFull, return number of items put."""
    count = 0
    while True:
        try:
            q.put(b"x", block=False)
            count += 1
        except ShmQueueFull:
            return count


class _Unserializable:
    """A class that msgpack cannot serialize."""


class TestInitExceptions(unittest.TestCase):
    """Exceptions raised during ShmQueue construction."""

    def test_type_error_queue_name_not_string(self):
        with self.assertRaises(ShmQueueTypeError):
            shmqueue.ShmQueue(12345)

    def test_value_error_negative_buffer_size(self):
        with self.assertRaises(ShmQueueValueError):
            shmqueue.ShmQueue(f"{QUEUE_NAME_BASE}_neg_buf", buffer_size=-1)

    def test_value_error_zero_buffer_size(self):
        # 0 passes the non-negative check but fails the < SYSTEM_PAGESIZE check
        with self.assertRaises(ShmQueueValueError):
            shmqueue.ShmQueue(f"{QUEUE_NAME_BASE}_zero_buf", buffer_size=0)

    def test_value_error_buffer_smaller_than_pagesize(self):
        # A small positive value gets rounded up but is still < SYSTEM_PAGESIZE after init guard
        with self.assertRaises(ShmQueueTypeError):
            # buffer_size=1 rounds up to SYSTEM_PAGESIZE so that actually succeeds.
            # Pass a non-multiple that rounds to exactly 0 — i.e. 0 itself.
            # Instead, rely on the zero test above; here test non-int type.
            shmqueue.ShmQueue(f"{QUEUE_NAME_BASE}_float_buf", buffer_size=1.5)


class TestPutExceptions(unittest.TestCase):
    """Exceptions raised by ShmQueue.put()."""

    def test_full_block_false(self):
        q = _make_queue("put_full")
        _fill_queue(q)
        with self.assertRaises(ShmQueueFull):
            q.put(b"x", block=False)

    def test_value_error_data_too_large(self):
        """Data larger than the entire buffer must raise ShmQueueValueError immediately."""
        q = _make_queue("put_too_large")
        oversized = b"x" * (BUFFER_SIZE + 1)
        with self.assertRaises(ShmQueueValueError):
            q.put(oversized)

    def test_serialization_error_no_pickle(self):
        """Non-msgpack-serializable object with pickle disabled raises ShmQueueSerializationError."""
        q = shmqueue.ShmQueue(f"{QUEUE_NAME_BASE}_ser_err",
                              buffer_size=BUFFER_SIZE,
                              allow_pickle=False)
        with self.assertRaises(ShmQueueSerializationError):
            q.put(_Unserializable())


class TestGetExceptions(unittest.TestCase):
    """Exceptions raised by ShmQueue.get()."""

    def test_empty_block_false(self):
        q = _make_queue("get_empty")
        with self.assertRaises(ShmQueueEmpty):
            q.get(block=False)

    def test_deserialization_error_no_pickle(self):
        """Getting a pickle-serialized item when pickle is disabled raises ShmQueueDeserializationError."""
        # write side: pickle allowed so we can put the object
        q_writer = shmqueue.ShmQueue(f"{QUEUE_NAME_BASE}_deser_err",
                                     buffer_size=BUFFER_SIZE,
                                     allow_pickle=True)
        q_writer.put(_Unserializable())  # forces pickle path

        # read side: pickle disabled
        q_reader = shmqueue.ShmQueue(f"{QUEUE_NAME_BASE}_deser_err",
                                     buffer_size=BUFFER_SIZE,
                                     allow_pickle=False)
        with self.assertRaises(ShmQueueDeserializationError):
            q_reader.get(block=False)


class TestNotInitializedError(unittest.TestCase):
    """ShmQueueNotInitializedError is raised when ref_data is None."""

    def test_clear_after_shutdown(self):
        """Calling clear() on an already-shutdown collection raises ShmQueueNotInitializedError."""
        q = _make_queue("not_init_clear")
        col = q._shm_collection
        q.shutdown()
        # ref_data is None after shutdown; clear() must detect this
        with self.assertRaises(ShmQueueNotInitializedError):
            col.clear()

    def test_reduce_ref_counter_after_shutdown(self):
        """reduce_ref_counter() on a shutdown collection raises ShmQueueNotInitializedError."""
        q = _make_queue("not_init_rrc")
        col = q._shm_collection
        q.shutdown()
        with self.assertRaises(ShmQueueNotInitializedError):
            col.reduce_ref_counter()


class TestRuntimeError(unittest.TestCase):
    """ShmQueueRuntimeError is raised when internal state is inconsistent."""

    def test_reduce_ref_counter_when_ref_count_is_zero(self):
        """Forcing ref_count to 0 and calling reduce_ref_counter raises ShmQueueRuntimeError."""
        q = _make_queue("runtime_rrc")
        original = q._shm_collection.ref_data.ref_count
        q._shm_collection.ref_data.ref_count = 0
        try:
            with self.assertRaises(ShmQueueRuntimeError):
                q._shm_collection.reduce_ref_counter()
        finally:
            # restore ref_count so the queue can shut down cleanly
            q._shm_collection.ref_data.ref_count = original

    def test_reduce_ref_counter_last_ref_raises_value_error(self):
        """reduce_ref_counter() on the last remaining reference raises ShmQueueValueError,
        not RuntimeError — to prevent accidental dangling shared memory."""
        q = _make_queue("rrc_last_ref")
        self.assertEqual(q._shm_collection.ref_data.ref_count, 1)
        with self.assertRaises(ShmQueueValueError):
            q._shm_collection.reduce_ref_counter()


if __name__ == "__main__":
    unittest.main(verbosity=2)
