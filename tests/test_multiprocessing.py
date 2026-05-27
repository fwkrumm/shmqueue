"""
tests of basics (lock/release) of shmlock package
"""
from multiprocessing import shared_memory
import multiprocessing.synchronize
import time
import unittest
import logging
import sys
import msgpack
import shmqueue
from shmqueue import SIZE_HEADER

BUFFER_SIZE = shmqueue.SYSTEM_PAGESIZE
NUM_PROCS = 10 # number of processes to test with
RUNS_ASYNC = 100 # number of runs for async test; increase in final implementation

ASYNC_TEST_DATA = b"dummy data"
TEST_QUEUE_NAME_ASYNC = "test_mp_async"
TEST_QUEUE_NAME = "test_mp"


def worker(data_queue: multiprocessing.Queue):
    """
    worker function for testing sequentially adding data to the queue
    this function will be called by each process and will get data from the data queue and
    put them into the shared memory queue.

    the shared memory queue object will be destroyed after the worker is done, so only
    the main process will prevent the shared memory queue underlying shared memory from
    being destroyed completely.

    Parameters
    ----------
    data_queue : multiprocessing.Queue
        _description_
    """
    #print(time.perf_counter(), "DEBUG: worker started")
    s = shmqueue.ShmQueue(TEST_QUEUE_NAME)
    #print(time.perf_counter(), "DEBUG: lock created")
    s.put(data_queue.get())
    #print(time.perf_counter(), "DEBUG: worker finished")

def multiple_worker():
    """

    worker function for testing multiple processes. each process will try to put data into the
    queue RUNS_ASYNC times

    Parameters
    ----------
    data_queue : multiprocessing.Queue
        _description_
    """
    s = shmqueue.ShmQueue(TEST_QUEUE_NAME_ASYNC)
    cnt = 0
    while cnt < RUNS_ASYNC:
        try:
            s.put(ASYNC_TEST_DATA, serialization_method=shmqueue.SerializationMethods.DEFAULT, block=False)
            cnt += 1
        except shmqueue.exceptions.ShmQueueFull:
            # catch explicitly here so that the loop finishes after RUNS_ASYNC runs,
            # otherwise it would run infinitely
            time.sleep(0.01)  # wait a bit if queue is full
            continue

class MultiprocessingTest(unittest.TestCase):
    """
    test of multiprocessing usage of shmqueue package

    Parameters
    ----------
    unittest : _type_
        _description_
    """

    @classmethod
    def setUpClass(cls):
        # compute the actual in-buffer footprint: header + msgpack-serialized payload
        serialized_size = len(msgpack.packb(ASYNC_TEST_DATA))
        item_size = SIZE_HEADER + serialized_size
        assert BUFFER_SIZE >= item_size, (
            f"SYSTEM_PAGESIZE ({BUFFER_SIZE}) is too small for tests; "
            f"at least {item_size} bytes required per item "
            f"({SIZE_HEADER} header + {serialized_size} msgpack payload)"
        )

    def __init__(self, *args, **kwargs):
        """
        test init method
        """
        super().__init__(*args, **kwargs)

    def test_mp_async(self):
        """
        test of multiple processes adding data to the queue asynchronously
        this test will start multiple processes that will add data to the queue
        each process will add a dummy data to the queue, the test will then
        check if the queue size is equal to the number of runs
        the test will also check if the data in the queue is correct, i.e. if
        the data is a list of integers from 0 to the index of the process
        this test will also check if the queue size is equal to the number of runs
        the test will also check if the queue is empty after all data has been added
        """
        s = shmqueue.ShmQueue(TEST_QUEUE_NAME_ASYNC, buffer_size=BUFFER_SIZE)
        procs = []

        t_start = time.perf_counter()
        for _ in range(NUM_PROCS):
            proc = multiprocessing.Process(target=multiple_worker)
            procs.append(proc)
            proc.start()

        data_received = 0
        while data_received < RUNS_ASYNC * NUM_PROCS:
            try:
                data = s.get(block=False)  # wait for data to be available
                self.assertEqual(data, ASYNC_TEST_DATA, "data in queue should be equal to "\
                    f"{ASYNC_TEST_DATA}, but is {data}")
                data_received += 1
            except shmqueue.exceptions.ShmQueueEmpty:
                # if queue is empty, we can continue waiting
                time.sleep(0.01)
                continue

        for proc in procs:
            # wait for all processes to finish
            proc.join()

        elapsed = time.perf_counter() - t_start
        total_items = RUNS_ASYNC * NUM_PROCS
        print(f"\n[test_mp_async] {total_items} items in {elapsed:.2f}s "
              f"({total_items / elapsed:.0f} items/s, {NUM_PROCS} procs)")

    def test_mp_sequentially(self):
        """
        test of multiple processes adding data to the queue sequentially
        this test will start multiple processes that will add data to the queue
        each process will add a list of data to the queue, the data will be
        a list of integers from 0 to the index of the process
        the data will be added to the queue in a way that each process will
        add its data to the queue before the next process starts
        the test will then check if the data in the queue is correct, i.e. if
        the data is a list of integers from 0 to the index of the process
        this test will also check if the queue size is equal to the number of processes
        """

        # to assure we have enough memory for the test, we calculate the size required
        size_required = sum([len(list(range(num))) for num in range(NUM_PROCS)]) \
            + SIZE_HEADER * NUM_PROCS
        # size will be rounded to at least being the system pagesize
        s = shmqueue.ShmQueue(TEST_QUEUE_NAME, buffer_size=size_required)
        queues = [multiprocessing.Queue() for _ in range(NUM_PROCS)]
        procs = []
        t_start = time.perf_counter()
        for mp_queue in queues:
            proc = multiprocessing.Process(target=worker, args=(mp_queue,))
            procs.append(proc)
            proc.start()

        # processes have been started, now we can add data to the queues
        for idx, mp_queue in enumerate(queues):
            # each worker will add exactly one element to the queue ans then return
            # print(time.perf_counter(), "DEBUG: adding data to queue:", idx)
            self._add_data_to_queue(mp_queue, list(range(idx)))
            # print(time.perf_counter(), "DEBUG: data added to queue:", idx)

        for proc in procs:
            proc.join()

        self.assertEqual(s.qsize(), NUM_PROCS, "queue size should be equal to number of processes")

        for idx in range(NUM_PROCS):
            data = s.get()
            self.assertIsInstance(data, list, "data should be a list")
            self.assertEqual(len(data), idx, f"data should contain {idx} elements but "\
                                             f"got {len(data)}")

        self.assertTrue(s.empty(), "queue should be empty after getting all data")

        elapsed = time.perf_counter() - t_start
        print(f"\n[test_mp_sequentially] {NUM_PROCS} items in {elapsed:.2f}s")


    def _add_data_to_queue(self, mp_queue: multiprocessing.Queue, data):
        """
        add data to a multiprocessing queue; function will then wait until the data
        have been removed from the queue by another process. we use this helper function
        to test sequentiall adding data to the queue

        Parameters
        ----------
        mp_queue : multiprocessing.Queue
            the queue to add data to
        data : any
            the data to add to the queue
        """
        self.assertEqual(mp_queue.qsize(), 0, "queue should be empty before adding data")
        # print(time.perf_counter(), "DEBUG: adding data of length", len(data), "to queue")
        s = shmqueue.ShmQueue(TEST_QUEUE_NAME)
        old_size = s.qsize()
        # print(time.perf_counter(), "DEBUG: old size of queue is", old_size)
        mp_queue.put(data)
        # print(time.perf_counter(), "DEBUG: data added to queue, waiting for it to be processed")
        while not s.qsize() == old_size + 1:
            # waiting until data have been put to shm queue, for this test we check that order
            # is maintained. infinite loop at the moment, add timeout later
            time.sleep(0.01)
        # print(time.perf_counter(), "DEBUG: _add_data_to_queue done")


if __name__ == "__main__":
    unittest.main(verbosity=2)
