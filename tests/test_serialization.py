"""
tests of basics (lock/release) of shmlock package
"""
from multiprocessing import shared_memory
import time
import unittest
import shmqueue
import struct
import queue

from shmqueue import SerializationMethods

TEST_QUEUE_NAME = "test_serialization_queue"

# for pickle
class testclass:
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


class TestSerialization(unittest.TestCase):
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

    def test_default(self):
        """
        test default serialization of shmqueue
        """
        s = shmqueue.ShmQueue(TEST_QUEUE_NAME, buffer_size=1000)

        self.assertEqual(s.refs, 1)
        self.assertEqual(s.qsize(), 0)

        s.put("test_default", serialization_method=SerializationMethods.DEFAULT) # msgpack
        s.put("test_msgpack", serialization_method=SerializationMethods.MSGPACK)
        s.put("test_pickle", serialization_method=SerializationMethods.PICKLE)

        self.assertEqual(s.qsize(), 3)
        self.assertEqual(s.get(), "test_default")
        self.assertEqual(s.get(), "test_msgpack")
        self.assertEqual(s.get(), "test_pickle")
        self.assertEqual(s.qsize(), 0)

        with self.assertRaises(NotImplementedError):
            # test with threshold value which will not be used internally
            s.put("test", serialization_method=SerializationMethods.FIRST_PUBLIC_NUMBER)

    def test_custom_serialization(self):
        """
        Custom serialization method for testing
        """
        s = shmqueue.ShmQueue(TEST_QUEUE_NAME, buffer_size=1000)

        self.assertEqual(s.refs, 1)

        # set int for serialization method
        serialization_method = SerializationMethods.FIRST_PUBLIC_NUMBER + 1

        # first test that default method is not implemented
        value_to_test = 123123123
        with self.assertRaises(NotImplementedError):
            # we did not yet define a proper serializer
            s.put(value_to_test, serialization_method=serialization_method)
        self.assertEqual(s.qsize(), 0)

        # result queue to check if custom serializer/deserializer is called
        result_queue = queue.Queue()

        # define custom deserializer
        def custom_deserializer(method: int, data: list[memoryview]):
            if method == serialization_method:
                result_queue.put("deserialized_data")
                # print(f"Custom deserializer called with data: {data} and len {len(data)}")
                data = b"".join(data) # no feeding available
                return struct.unpack("I", data[:])[0]
            raise NotImplementedError("Custom deserialization method not implemented")

        # define custom serializer
        def custom_serializer(method: int, data: any):
            if method == serialization_method:
                result_queue.put("serialized_data")
                # print(f"Custom serializer called with data: {data}")
                return struct.pack("I", data)
            raise NotImplementedError("Custom serialization method not implemented")

        # set custom serializer and deserializer
        s.custom_deserialize(custom_deserializer)
        s.custom_serialize(custom_serializer)

        # check that we still get NotImplementedError if we try to use
        # the wrong serialization method number
        with self.assertRaises(NotImplementedError):
            s.put(value_to_test, serialization_method=serialization_method+1)
        self.assertEqual(s.qsize(), 0)

        # use custom serializer and check that function was called
        s.put(value_to_test, serialization_method=serialization_method)
        # wait for data be put to the queue
        while result_queue.qsize() != 1:
            time.sleep(0.1)

        # check values and that result is again extracted via custom deserializer
        self.assertTrue("serialized_data" in result_queue.queue)
        self.assertEqual(s.qsize(), 1)
        self.assertEqual(s.get(), value_to_test)

        # check that deserializer was called
        while result_queue.qsize() != 2:
            time.sleep(0.1)
        self.assertTrue("deserialized_data" in result_queue.queue)

        # since we called get the queue should be empty now
        self.assertEqual(s.qsize(), 0)

    def test_custom_serialization_raw_bytes(self):
        """
        Custom serialization method for testing with raw bytes i.e. if serialization
        is done elsewhere
        """
        queue_name = str(time.time())
        s = shmqueue.ShmQueue(queue_name, buffer_size=1000)

        self.assertEqual(s.refs, 1)

        # set int for serialization method
        serialization_method = SerializationMethods.FIRST_PUBLIC_NUMBER + 1

        # first test that default method is not implemented
        value_to_test = b"123123123"
        with self.assertRaises(NotImplementedError):
            # we did not yet define a proper serializer
            s.put(value_to_test, serialization_method=serialization_method)
        self.assertEqual(s.qsize(), 0)

        # result queue to check if custom serializer/deserializer is called
        result_queue = queue.Queue()

        # define custom deserializer
        def custom_deserializer(method: int, data: memoryview):
            if method == serialization_method:
                result_queue.put("deserialized_data")
                data = b"".join(data) # no feeding available
                return bytearray(data)
            raise NotImplementedError("Custom deserialization method not implemented")

        # define custom serializer
        def custom_serializer(method: int, data: any):
            if method == serialization_method:
                result_queue.put("serialized_data")
                return bytes(data)
            raise NotImplementedError("Custom serialization method not implemented")

        # set custom serializer and deserializer
        s.custom_deserialize(custom_deserializer)
        s.custom_serialize(custom_serializer)

        # use custom serializer and check that function was called
        s.put(value_to_test, serialization_method=serialization_method)

        # wait for data be put to the queue
        while result_queue.qsize() != 1:
            time.sleep(0.1)

        # check values and that result is again extracted via custom deserializer
        self.assertTrue("serialized_data" in result_queue.queue)
        self.assertEqual(s.qsize(), 1)
        self.assertEqual(list(s.get()), list(bytearray(value_to_test)))

        # check that deserializer was called
        while result_queue.qsize() != 2:
            time.sleep(0.1)
        self.assertTrue("deserialized_data" in result_queue.queue)

        # since we called get the queue should be empty now
        self.assertEqual(s.qsize(), 0)

if __name__ == "__main__":
    unittest.main(verbosity=2)
