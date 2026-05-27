"""
class with buffer operations for shmqueue
"""

from dataclasses import dataclass
import struct
from typing import Union
from shmqueue.exceptions import ShmQueueValueError

# basically the header:
TYPE_PAYLOAD_LENGTH = "I"
TYPE_SERIALIZATION_METHOD = "I"
SIZE_HEADER = struct.calcsize(TYPE_PAYLOAD_LENGTH) + struct.calcsize(TYPE_SERIALIZATION_METHOD)

RESERVED_SERIALIZATION_METHOD_THRESHOLD = 1000 # all values up to this
                                               # value are reserved for internal use

@dataclass
class SerializationMethods:
    """
    Enum for serialization methods used in the shared memory queue.
    """
    MSGPACK = 0             # default
    DEFAULT = MSGPACK       # default serialization method
    PICKLE = 1              # as fallback if MSGPACK fails
    # 0 .. RESERVED_SERIALIZATION_METHOD_THRESHOLD are reserved for internal use
    FIRST_PUBLIC_NUMBER = RESERVED_SERIALIZATION_METHOD_THRESHOLD+1



@dataclass
class ShmQueueHeader:
    """
    header for the shared memory queue
    This header is used to store metadata about the data being sent through the queue.
    It contains the payload length and the serialization method used to serialize the data.
    Attributes
    ----------
    payload_length : int
        length of the payload in bytes

    serialization_method : int
        method used to serialize the data. zero is reserved for pickle, the rest can
          be defined by the

    size : int
        size of the header in bytes, this is required for serialization and deserialization
    Methods

    -------
    serialize() -> bytes
        serialize the header to bytes

    deserialize(data: bytes)
        deserialize the header from bytes

    from_bytes(data: bytes) -> ShmQueueHeader
        create a ShmQueueHeader object from bytes

    """
    payload_length: int = 0
    serialization_method: int = SerializationMethods.DEFAULT
    size = SIZE_HEADER # required?

    def serialize(self) -> bytes:
        """
        serialize the header to bytes
        """
        return struct.pack(TYPE_PAYLOAD_LENGTH, self.payload_length) + \
               struct.pack(TYPE_SERIALIZATION_METHOD, self.serialization_method)

    def deserialize(self, data: Union[bytes, bytearray, memoryview]):
        """
        deserialize the header from bytes
        """
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise ShmQueueValueError("data must be bytes, bytearray, "\
                                     f"or memoryview but got {type(data)}")
        if len(data) != SIZE_HEADER:
            raise ShmQueueValueError(f"data length is {len(data)} but "\
                                     f"should be {SIZE_HEADER}")
        self.payload_length = struct.unpack(TYPE_PAYLOAD_LENGTH,
                                            data[0:struct.calcsize(TYPE_PAYLOAD_LENGTH)])[0]
        self.serialization_method = struct.unpack(TYPE_SERIALIZATION_METHOD,
                                                  data[struct.calcsize(TYPE_PAYLOAD_LENGTH):])[0]

    @classmethod
    def from_bytes(cls, data: bytes):
        """
        create header object from bytes
        """
        # type checks happen in deserialize function
        header = cls()
        header.deserialize(data)
        return header
