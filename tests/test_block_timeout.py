"""
Tests for the block and timeout parameters of ShmQueue.put() and ShmQueue.get().

AI GENERATED
"""
import multiprocessing
import time
import unittest
import shmqueue
from shmqueue.exceptions import ShmQueueEmpty, ShmQueueFull

QUEUE_NAME_BASE = "test_block_timeout"
DATA = b"hello"

# buffer just large enough for a small number of items so we can fill it quickly
BUFFER_SIZE = shmqueue.SYSTEM_PAGESIZE

# delay used by helper processes to unblock a waiting call
UNBLOCK_DELAY = 0.1  # seconds


def _make_queue(suffix=""):
    return shmqueue.ShmQueue(f"{QUEUE_NAME_BASE}_{suffix}", buffer_size=BUFFER_SIZE)


def _fill_queue(q: shmqueue.ShmQueue):
    """Put items until ShmQueueFull is raised, then return the count put."""
    count = 0
    while True:
        try:
            q.put(DATA, block=False)
            count += 1
        except ShmQueueFull:
            return count


# --- helper entry points for child processes ---

def _helper_put(queue_name, delay):
    """Attach to the named queue, sleep, then put one item."""
    time.sleep(delay)
    q = shmqueue.ShmQueue(queue_name, buffer_size=BUFFER_SIZE)
    q.put(DATA)


def _helper_get(queue_name, delay):
    """Attach to the named queue, sleep, then get one item."""
    time.sleep(delay)
    q = shmqueue.ShmQueue(queue_name, buffer_size=BUFFER_SIZE)
    q.get(block=True)


class TestBlockFalse(unittest.TestCase):
    """block=False must raise immediately on empty / full queues."""

    def test_get_block_false_empty_raises(self):
        q = _make_queue("get_nblock_empty")
        with self.assertRaises(ShmQueueEmpty):
            q.get(block=False)

    def test_put_block_false_full_raises(self):
        q = _make_queue("put_nblock_full")
        _fill_queue(q)
        with self.assertRaises(ShmQueueFull):
            q.put(DATA, block=False)

    def test_get_block_false_is_fast(self):
        """A non-blocking get on an empty queue must return essentially instantly."""
        q = _make_queue("get_nblock_fast")
        t0 = time.perf_counter()
        with self.assertRaises(ShmQueueEmpty):
            q.get(block=False)
        elapsed = time.perf_counter() - t0
        self.assertLess(elapsed, 0.05, "block=False get should return in <50 ms")

    def test_put_block_false_is_fast(self):
        """A non-blocking put on a full queue must return essentially instantly."""
        q = _make_queue("put_nblock_fast")
        _fill_queue(q)
        t0 = time.perf_counter()
        with self.assertRaises(ShmQueueFull):
            q.put(DATA, block=False)
        elapsed = time.perf_counter() - t0
        self.assertLess(elapsed, 0.05, "block=False put should return in <50 ms")

    def test_get_block_false_nonempty_succeeds(self):
        q = _make_queue("get_nblock_ok")
        q.put(DATA)
        result = q.get(block=False)
        self.assertEqual(result, DATA)

    def test_put_block_false_nonfull_succeeds(self):
        q = _make_queue("put_nblock_ok")
        q.put(DATA, block=False)  # must not raise
        self.assertEqual(q.qsize(), 1)


class TestBlockTrue(unittest.TestCase):
    """block=True must wait until the operation can proceed."""

    def test_get_block_true_waits_for_producer(self):
        """get(block=True) on an empty queue must wait until another process puts data."""
        q = _make_queue("get_block_wait")

        p = multiprocessing.Process(
            target=_helper_put,
            args=(f"{QUEUE_NAME_BASE}_get_block_wait", UNBLOCK_DELAY),
            daemon=True,
        )
        p.start()

        t0 = time.perf_counter()
        result = q.get(block=True)
        elapsed = time.perf_counter() - t0

        p.join()
        self.assertEqual(result, DATA)
        self.assertGreaterEqual(elapsed, UNBLOCK_DELAY * 0.8,
                                "get(block=True) should have waited for the producer")

    def test_get_block_true_nonempty_succeeds(self):
        """get(block=True) on a non-empty queue must return the item without waiting."""
        q = _make_queue("get_block_nonempty")
        q.put(DATA)
        result = q.get(block=True)
        self.assertEqual(result, DATA)
        self.assertTrue(q.empty())

    def test_put_block_true_nonfull_succeeds(self):
        """put(block=True) on a non-full queue must store the item without waiting."""
        q = _make_queue("put_block_nonfull")
        q.put(DATA, block=True)
        self.assertEqual(q.qsize(), 1)

    def test_put_block_true_waits_for_consumer(self):
        """put(block=True) on a full queue must wait until another process gets data."""
        q = _make_queue("put_block_wait")
        _fill_queue(q)

        p = multiprocessing.Process(
            target=_helper_get,
            args=(f"{QUEUE_NAME_BASE}_put_block_wait", UNBLOCK_DELAY),
            daemon=True,
        )
        p.start()

        t0 = time.perf_counter()
        q.put(DATA, block=True)
        elapsed = time.perf_counter() - t0

        p.join()
        self.assertGreaterEqual(elapsed, UNBLOCK_DELAY * 0.8,
                                "put(block=True) should have waited for the consumer")


class TestTimeoutPassthrough(unittest.TestCase):
    """
    timeout is forwarded to the underlying lock acquisition.
    These tests verify that passing timeout does not break normal operation.
    """

    def test_get_with_timeout_succeeds_when_data_available(self):
        q = _make_queue("get_timeout_ok")
        q.put(DATA)
        result = q.get(block=True, timeout=1.0)
        self.assertEqual(result, DATA)

    def test_put_with_timeout_succeeds_when_space_available(self):
        q = _make_queue("put_timeout_ok")
        q.put(DATA, block=True, timeout=1.0)
        self.assertEqual(q.qsize(), 1)

    def test_get_block_false_timeout_ignored(self):
        """With block=False, timeout is irrelevant; ShmQueueEmpty raised immediately."""
        q = _make_queue("get_nblock_timeout")
        with self.assertRaises(ShmQueueEmpty):
            q.get(block=False, timeout=5.0)

    def test_put_block_false_timeout_ignored(self):
        """With block=False, timeout is irrelevant; ShmQueueFull raised immediately."""
        q = _make_queue("put_nblock_timeout")
        _fill_queue(q)
        with self.assertRaises(ShmQueueFull):
            q.put(DATA, block=False, timeout=5.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
