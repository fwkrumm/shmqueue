"""
class which manages the shared memory blocks
"""
import mmap
import struct
from collections import namedtuple
from multiprocessing import shared_memory


# we create shared memory blocks with the size of the system pagesize
# since when you append to shared memory you have to append in multiples of the system pagesize
# NOTE  this is not by any means memory efficient but at least on Windows, that is the way it goes.
SYSTEM_PAGESIZE = mmap.PAGESIZE # default if not specified

ShmRefDataSnapshot = namedtuple(
    "ShmRefDataSnapshot",
    ["ref_count", "put_pos", "get_pos", "buffer_size", "qsize", "same_lap"],
)

# for simplicity we use the same type for all variables (uint32)
TYPE_SHM_DATA = "I"
SIZE_SHM_DATA = struct.calcsize(TYPE_SHM_DATA)

# number of data stored for each shm queue
NUMBER_SHM_DATA = 6

assert NUMBER_SHM_DATA*SIZE_SHM_DATA < SYSTEM_PAGESIZE, \
    "shared memory data must be smaller than system pagesize"

class ShmRefData:
    """
    collectively store all required shared memory data

    make sure number of parameters is equal to NUMBER_SHM_DATA

    NOTE synchronization i.e. lock mechanism is NOT part of this class i.e. it
    has to be provided externally.
    """

    # Byte offset of each field inside the shared memory buffer.
    # All fields are uint32 (SIZE_SHM_DATA bytes), laid out sequentially.
    # uint32 is highly over-dimensioned for same_lap, but kept for uniformity.
    _POS_REF_COUNT   = 0 * SIZE_SHM_DATA
    _POS_PUT_POS     = 1 * SIZE_SHM_DATA
    _POS_GET_POS     = 2 * SIZE_SHM_DATA
    _POS_BUFFER_SIZE = 3 * SIZE_SHM_DATA
    _POS_QSIZE       = 4 * SIZE_SHM_DATA
    _POS_SAME_LAP    = 5 * SIZE_SHM_DATA

    def __init__(self, buffer: memoryview):

        self._buf = buffer

    #
    # getter/setter for dynamic data
    #

    @property
    def buffer_occupancy(self) -> int:
        """
        get current buffer occupancy

        Returns
        -------
        int
            current buffer occupancy in bytes
        """
        put_pos = self.put_pos
        get_pos = self.get_pos
        same_lap = self.same_lap
        buffer_size = self.buffer_size
        # full: pointers equal but on different laps
        if put_pos == get_pos and same_lap == 0:
            return buffer_size
        if put_pos < get_pos:
            return put_pos + buffer_size - get_pos
        return put_pos - get_pos

    @property
    def ref_count(self) -> int:
        return self._get_value_at_byte_pos(self._POS_REF_COUNT)

    @ref_count.setter
    def ref_count(self, value: int):
        self._set_value_at_byte_pos(self._POS_REF_COUNT, value)

    @property
    def put_pos(self) -> int:
        return self._get_value_at_byte_pos(self._POS_PUT_POS)

    @put_pos.setter
    def put_pos(self, value: int):
        self._set_value_at_byte_pos(self._POS_PUT_POS, value)

    @property
    def get_pos(self) -> int:
        return self._get_value_at_byte_pos(self._POS_GET_POS)

    @get_pos.setter
    def get_pos(self, value: int):
        self._set_value_at_byte_pos(self._POS_GET_POS, value)

    @property
    def buffer_size(self) -> int:
        return self._get_value_at_byte_pos(self._POS_BUFFER_SIZE)

    @buffer_size.setter
    def buffer_size(self, value: int):
        self._set_value_at_byte_pos(self._POS_BUFFER_SIZE, value)

    @property
    def qsize(self) -> int:
        return self._get_value_at_byte_pos(self._POS_QSIZE)

    @qsize.setter
    def qsize(self, value: int):
        self._set_value_at_byte_pos(self._POS_QSIZE, value)

    @property
    def same_lap(self) -> int:
        return self._get_value_at_byte_pos(self._POS_SAME_LAP)

    @same_lap.setter
    def same_lap(self, value: int):
        self._set_value_at_byte_pos(self._POS_SAME_LAP, value)

    def return_everything(self) -> tuple[int, int, int, int, int, int]:
        """
        return all values stored in shared memory

        Returns
        -------
        tuple
            tuple of all values stored in shared memory namely
            ref_count, put_pos, get_pos, buffer_size, qsize, same_lap
        """
        return struct.unpack_from(str(NUMBER_SHM_DATA) + TYPE_SHM_DATA, self._buf, 0)

    def __repr__(self):
        return f"ref_count: {self.ref_count}, get_pos: {self.get_pos}, put_pos: {self.put_pos}, "\
               f"buffer_size: {self.buffer_size}, qsize: {self.qsize}, same_lap: {self.same_lap}"

    def __str__(self):
        return self.__repr__()

    #
    # private methods
    #

    def _get_value_at_byte_pos(self, byte_pos: int) -> int:
        return struct.unpack_from(TYPE_SHM_DATA, self._buf, byte_pos)[0]

    def _set_value_at_byte_pos(self, byte_pos: int, value: int):
        struct.pack_into(TYPE_SHM_DATA, self._buf, byte_pos, value)

    #
    # archive methods to set multiple values at once; might still use old variables
    #

    #def decode(self):
    #    self.ref_count, self.put_pos, self.get_pos, \
    #           self.buffer_size, self.chunk_size, self.qsize = \
    #        struct.unpack_from(NUMBER_SHM_DATA*TYPE_SHM_DATA, self._buf, 0)

    #def encode(self):
    #    struct.pack_into(NUMBER_SHM_DATA*TYPE_SHM_DATA, self._buf, 0,
    #                     self.ref_count,
    #                     self.put_pos,
    #                     self.get_pos,
    #                     self.buffer_size,
    #                     self.chunk_size,
    #                     self.qsize)

if __name__ == "__main__":
    import time
    # for testing
    shm = shared_memory.SharedMemory(name="test", create=True, size=SYSTEM_PAGESIZE)

    data = ShmRefData(shm.buf)
    a = time.time()
    data.ref_count += 1
    data.ref_count += 1
    b = time.time()
    v = data.ref_count
    print(b-a)
    print(v)

    print(list(shm.buf[0:4]))

    # clean up
    shm.close()
    shm.unlink()
