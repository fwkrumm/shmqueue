"""
class which manages the shared memory blocks i.e. creates/attaches and closes them
"""
import datetime
import json
import logging
import mmap
import os
import pickle
import sys
import time
import warnings
from dataclasses import dataclass
from enum import Enum
from multiprocessing import shared_memory
from typing import Union

import msgpack

# pylint: disable=wrong-import-position
import shmlock
from shmqueue.base_logger import ShmModuleBaseLogger
from shmqueue.exceptions import ShmQueueRuntimeError, ShmQueueValueError, ShmQueueFull, \
    ShmQueueEmpty, ShmQueueSerializationError, ShmQueueDeserializationError, \
        ShmQueueNotInitializedError
from shmqueue.shared_memory_data import ShmRefData, ShmRefDataSnapshot
from shmqueue.buffer_operations import ShmQueueHeader, \
    SIZE_HEADER, RESERVED_SERIALIZATION_METHOD_THRESHOLD, SerializationMethods
# pylint: enable=wrong-import-position

# we create shared memory blocks with the size of the system pagesize
# since when you append to shared memory you have to append in multiples of the system pagesize
# NOTE this is not by any means memory efficient but at least on Windows, that is the way it goes.
SYSTEM_PAGESIZE = mmap.PAGESIZE # default if not specified

# to prevemt spam of the resource tracker on posix systems we remove the shm lock name
# from resource tracker; this requires a patching some methods within the tracker.
# set the following to False to disable this behavior. This however might lead to
# false positive warnings of the resource tracker
DISABLE_RESOURCE_TRACKING = True

# in seconds; this is the time the process will sleep if queues is full and block is true
# and queue is empty and block is True
BLOCK_SLEEP_TIME = 0.01

@dataclass
class ShmMode:
    """
    just to handle returns of shm queue to determine whether shm has been created
    or if it has been attached
    """
    attach: int = 0
    create: int = 1


class DebugRefLogAction(Enum):
    """
    Actions supported by debug_ref_log.
    """
    CREATE = "create"
    SHUTDOWN = "shutdown"

class ShmCollection(ShmModuleBaseLogger):
    """
    class which manages the shared memory blocks i.e. creates/attaches and closes them

    Parameters
    ----------
    ShmModuleBaseLogger : _type_
        base logger which is used for logging within the shared memory collection class
    """

    def __init__(self,  # pylint: disable=too-many-positional-arguments
                 name: str,
                 buffer_size: int,
                 use_pickle: bool,
                 track_resources: bool = False,
                 logger: logging.Logger = None):

        super().__init__(logger=logger)

        # data operations on shm data name
        self.ref_data: ShmRefData = None

        # set names for shared memory blocks
        shm_ref_data_name = f"{name}_data"
        self.shm_ref_data: shared_memory.SharedMemory = None

        # shm for queue payload
        self.shm_payload: shared_memory.SharedMemory = None

        # store pid for creation process
        self.pid = os.getpid()

        # create lock instance
        lock_name = f"{name}_lock"

        self._pickle = use_pickle
        # track_resources=True re-enables Python's resource tracker for shared-memory blocks;
        # False (default) suppresses false-positive warnings on POSIX and Python < 3.13.
        self._track_resources = track_resources

        if os.name == "posix" and not track_resources and sys.version_info < (3, 13):
            # patch resource tracker for name_lock, name_data and name shared memory blocks
            # NOTE that from python version 3.13 onwards the track parameter can be used (elsewhere)
            shmlock.remove_shm_from_resource_tracker(name)

        self._shm_lock = shmlock.ShmLock(lock_name,
                                         logger=logger,
                                         block_signals=True,
                                         memory_barrier=True,
                                         track=track_resources \
                                            if sys.version_info >= (3, 13) else None)

        with self._shm_lock.lock():

            # create shared memory keys which are required for synchronization
            rc, self.shm_ref_data = self._create_or_attach_shm(name=shm_ref_data_name,
                                                               size=SYSTEM_PAGESIZE)

            rc_shm, self.shm_payload = self._create_or_attach_shm(name=name,
                                                                  size=buffer_size)

            if rc != rc_shm:
                raise ShmQueueRuntimeError("shared memory blocks have been "\
                                           "created with different modes.")

            self.ref_data = ShmRefData(self.shm_ref_data.buf)

            # init shared memory data especially the reference count

            if rc == ShmMode.create:
                # set buffer size and init ref count with 1
                self.ref_data.buffer_size = buffer_size
                self.ref_data.ref_count = 1
                self.ref_data.same_lap = 1 # put pos and same pos are in the same cyclic buffer lap
                self.info("pid %s: Created shared memory collection %s. "\
                          "Make sure that at least one valid reference exists since "\
                          "otherwise shared memory will get released.", self.pid, name)
            elif rc == ShmMode.attach:
                if self.ref_data.buffer_size == 0:
                    raise ShmQueueValueError("buffer size has not been set during creation step.")
                self.ref_data.ref_count += 1
                self.info("pid %s: Attached to shared memory collection %s; "\
                          "ref count is now %s", self.pid, name, self.ref_data.ref_count)
            else:
                raise ShmQueueRuntimeError("shm mode is neither create nor attach.")

        self.info("pid %s: initialized queue with buffer size %s",
                  self.pid,
                  self.ref_data.buffer_size)


    def shutdown(self):
        """
        shutdown shared memory collection and release all shared memory blocks

        Raises
        ------
        ShmQueueRuntimeError
            Raised if there is an inconsistency in the shared memory state.
        ShmQueueValueError
            Raised if the reference count is already 0.
        """

        with self._shm_lock.lock():

            if self.ref_data is None or self.shm_ref_data is None:

                if self.ref_data is not None or self.shm_ref_data is not None:
                    raise ShmQueueRuntimeError("either _shm_data or _shm is None but not both. "\
                                               "They should be released together.")
                # prevent being called twice
                return

            if self.ref_data.ref_count == 0:
                raise ShmQueueValueError("ref count is already 0 i.e. cannot further reduce. "\
                                         "This should not happen.")

            self.ref_data.ref_count -= 1
            use_unlink = False

            # query if last reference has been closed
            if self.ref_data.ref_count == 0:
                self.info("Closing last reference of shared memory queue %s now.",
                                  self.shm_payload.name)
                if os.name == "posix":
                    # only supported on linux
                    use_unlink = True
            else:
                self.info("Closing shared memory queue %s. Ref count after(!) "\
                                  "reduction is %s", self.shm_payload.name, self.ref_data.ref_count)

            # attending all shared memory blocks
            for shm in (self.shm_ref_data, self.shm_payload,):

                self.debug("Closing shared memory %s", shm.name)

                # on windows shm gets released if all handles have been closed.
                shm.close()

                if use_unlink is True:
                    # posix only
                    try:
                        shm.unlink()
                        self.info("Unlinked shared %s", shm.name)
                    except FileNotFoundError:
                        self.warning("Shared memory %s already unlinked.", shm.name)
            # set to None to indicate that shared memory has been closed
            self.ref_data = None
            self.shm_payload = None
            self.shm_ref_data = None

    def debug_ref_log(self, path, action: DebugRefLogAction):
        """
        debug method to log reference count and shared memory data to json file
        """
        if not isinstance(action, DebugRefLogAction):
            raise ShmQueueValueError(f"action must be a DebugRefLogAction enum member, "
                                     f"but got {type(action)}")

        if self.ref_data is None:
            if action is DebugRefLogAction.CREATE:
                # if garbage collection is performed at some other time after shutdown it is
                # possible that this is executed twice
                raise ShmQueueNotInitializedError("shared memory has not been initialized yet.")
            # SHUTDOWN action: ref_data already cleared; nothing meaningful to log
            return

        with self._shm_lock.lock():
            # try open json to get data
            try:
                with open(path, "r", encoding="utf-8") as f:
                    json_data = json.load(f)
            except FileNotFoundError:
                # first queue do debug log, NOTE that there might be still other references!
                # debug logging has to be enabled for all queues separately. However we could
                # add that the master queue determines that?
                json_data = {}
            # since each queue has exactly ONE lock which has an unique uuid, we can use this as
            # identifier for the queue
            uuid = self._shm_lock.uuid

            if action is DebugRefLogAction.CREATE:

                if uuid in json_data:
                    del json_data[uuid]  # remove old data

                json_data[uuid] = {
                    "created": datetime.datetime.now().isoformat(),
                    "ref_count": self.ref_data.ref_count
                }
            elif action is DebugRefLogAction.SHUTDOWN:

                if uuid not in json_data:
                    # if not in json data, we add it
                    self.warning("uuid %s not in json data (should be!), adding it now.", uuid)

                    json_data[uuid] = {
                        "shutdown": datetime.datetime.now().isoformat(),
                        "ref_count": self.ref_data.ref_count
                    }

                else:
                    # if in json data, we update the shutdown time
                    json_data[uuid]["shutdown"] = datetime.datetime.now().isoformat()

            # update file
            with open(path, "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=4)

            self.info("Debug log written to %s", path)

    def clear(self):
        """
        clear shared memory queue. this will reset the put and get position
        and the same lap flag. it will not change the reference count.
        """
        with self._shm_lock.lock():
            if self.ref_data is None:
                raise ShmQueueNotInitializedError("shared memory has not been initialized yet.")

            self.info("Clearing shared memory queue %s", self.shm_payload.name)
            self.ref_data.put_pos = 0
            self.ref_data.get_pos = 0
            self.ref_data.same_lap = 1
            self.ref_data.qsize = 0

    def __del__(self):
        # make sure that shared memory is closed; this is not guaranteed though and only a fallback.
        # generally the user should call .shutdown() explicitly
        try:
            self.shutdown()
        except RuntimeError:
            # possible if the lock has been acquired and __del__ is called due to keyboard
            # interrupt
            self._shm_lock.release()
            self.shutdown()
        except TypeError:
            # do we want a warning here?
            warnings.warn("TypeError in __del__ method. This happens if a module is already "\
                          "unloaded before it is been used. It is recommended to call close() "\
                          "explicitly before termination.", RuntimeWarning)

    def add_ref_manually(self, pid: int):
        """
        add reference count manually. this is useful if the shared memory is shared
        via inheritance (only possible on posix systems)

        However this is STRONGLY discouraged because it might lead to leaked memory

        Parameters
        ----------
        pid : int
            process id of the process which should increment the reference count
        """
        with self._shm_lock.lock():
            if self.ref_data is None:
                raise ShmQueueNotInitializedError("shared memory has not been initialized or "\
                                                  "already shut down.")
            self.ref_data.ref_count += 1
            self.info("Changing pid %s -> %s. New ref count is %s",
                      self.pid, pid, self.ref_data.ref_count)
            self.pid = pid

    def _serialize_with_fallback(self, serialization_method: int, data: any) -> tuple[bytes, int]:
        """Serialize *data*; falls back to pickle on serialization error.

        Returns
        -------
        tuple[bytes, int]
            Serialized payload and the effective serialization method used.
        """
        try:
            return self._serialize(serialization_method, data), serialization_method
        except ShmQueueSerializationError:
            self.info("serialization method %s failed, trying fallback method.",
                      serialization_method)
            fallback = SerializationMethods.PICKLE
            return self._serialize(fallback, data), fallback

    def _is_buffer_full(self,
                        put_pos: int,
                        get_pos: int,
                        same_lap: int,
                        put_pos_after: int) -> bool:
        """Return True if writing up to *put_pos_after* would overflow the ring buffer.

        Three disjoint overflow conditions are checked:
        1. The new write end lands inside unconsumed data on the same lap.
        2. The write pointer is behind the read pointer (different laps) and the new write end
           would either cross get_pos or wrap a second time.
        3. The pointers are equal but on different laps (buffer is completely full).
        """
        if get_pos < put_pos_after <= put_pos:
            return True
        if same_lap == 0 and put_pos < get_pos and \
                (put_pos_after > get_pos or put_pos_after < put_pos):
            return True
        if same_lap == 0 and put_pos == get_pos:
            return True
        return False

    def _write_item(self,
                    put_pos: int,
                    put_pos_after: int,
                    buffer_size: int,
                    *,
                    header: bytes,
                    serialized_data: bytes,
                    same_lap: int) -> int:
        """Write *header* + *serialized_data* at *put_pos* in the ring buffer.

        Handles all wrap-around cases:
        - Contiguous write — no lap boundary crossed.
        - Payload wraps; header fits entirely at the end of the buffer.
        - Header itself straddles the end of the buffer (must be written in two slices).

        Returns
        -------
        int
            Updated same_lap value (toggled whenever a lap boundary is crossed).
        """
        if put_pos_after <= put_pos:
            # crossed the ring-buffer boundary; toggle the lap flag
            same_lap = int(not same_lap)  # always 0 or 1
            self.debug("did a complete lap; same lap is now %s", same_lap)

            if buffer_size - put_pos < SIZE_HEADER:
                # header itself straddles the end of the buffer — write it in two slices
                self.debug("not enough space for payload length; we have to split the data")
                self.shm_payload.buf[put_pos:buffer_size] = header[0:buffer_size - put_pos]
                self.shm_payload.buf[0:SIZE_HEADER - (buffer_size - put_pos)] \
                    = header[buffer_size - put_pos:]
                self.shm_payload.buf[SIZE_HEADER - (buffer_size - put_pos):put_pos_after] \
                    = serialized_data[:]
            else:
                # header fits at the end; only the payload wraps around
                self.shm_payload.buf[put_pos:put_pos + SIZE_HEADER] = header
                self.shm_payload.buf[put_pos + SIZE_HEADER:buffer_size] \
                    = serialized_data[0:buffer_size - put_pos - SIZE_HEADER]
                self.shm_payload.buf[0:put_pos_after] \
                    = serialized_data[buffer_size - SIZE_HEADER - put_pos:]
        else:
            # contiguous write — no lap boundary crossed
            self.shm_payload.buf[put_pos:put_pos + SIZE_HEADER] = header
            self.shm_payload.buf[put_pos + SIZE_HEADER:put_pos_after] = serialized_data

        return same_lap

    def put(self,  # pylint: disable=too-many-locals
            data: any,
            block: bool,
            timeout: Union[float, int, None],
            serialization_method: int) -> None:
        """
        put data into shared memory queue

        Parameters
        ----------
        data : any
            data to be put into shared memory queue
        block : bool
            whether to block if queue is full
        timeout : Union[float, int, None]
            maximum time to wait for an item if block is True; None means wait indefinitely
        serialization_method : int
            serialization method to be used for data serialization,
            by default SerializationMethods.DEFAULT (msgpack)
        """
        if not isinstance(serialization_method, int):
            raise ShmQueueValueError("serialization method must be of type "\
                                     f"int, but got {type(serialization_method)}")

        if self.ref_data is None:
            raise ShmQueueNotInitializedError("shared memory has not been initialized or "\
                                              "already shut down.")

        serialized_data, serialization_method = self._serialize_with_fallback(
            serialization_method, data)

        data_size = len(serialized_data) + SIZE_HEADER

        if data_size > self.ref_data.buffer_size:
            # no way this fits into queue at any given time
            raise ShmQueueValueError("data size is larger than overall buffer size.")

        self.debug("putting data of size %s into queue; serialized data is of length %s",
                   data_size, len(serialized_data))

        header = ShmQueueHeader(payload_length=len(serialized_data),
                                serialization_method=serialization_method).serialize()

        blocked = False
        deadline = (time.monotonic() + timeout) if (block and timeout is not None) else None

        while True:  # exit event?

            if blocked:
                # sleep so other processes can acquire the queue and consume data
                time.sleep(BLOCK_SLEEP_TIME)

            lock_timeout = (max(0.0, deadline - time.monotonic())
                            if deadline is not None else timeout)
            with self._shm_lock.lock(timeout=lock_timeout):

                ref_data: tuple = self.ref_data.return_everything()

                put_pos     = ref_data[1]
                get_pos     = ref_data[2]
                buffer_size = ref_data[3]
                same_lap    = ref_data[5]
                self.debug("ref data: %s", self.ref_data)

                # projected write-end position (modulo ring-buffer size)
                put_pos_after = (put_pos + data_size) % buffer_size

                if self._is_buffer_full(put_pos, get_pos, same_lap, put_pos_after):
                    if not block:
                        raise ShmQueueFull(f"buffer is full; put_pos_after = {put_pos_after}")
                    if deadline is not None and time.monotonic() >= deadline:
                        raise ShmQueueFull(
                            f"put timed out: buffer still full after timeout {timeout}s")
                    self.debug("buffer is full; put_pos_after = %s", put_pos_after)
                    blocked = True
                    continue

                same_lap = self._write_item(
                    put_pos, put_pos_after, buffer_size,
                    header=header, serialized_data=serialized_data, same_lap=same_lap)

                self.ref_data.same_lap = same_lap
                self.ref_data.put_pos = put_pos_after
                self.ref_data.qsize += 1

                self.debug("put data into queue; new put pos: %s", put_pos_after)
                return  # break out of while True

    def get(  # pylint: disable=too-many-locals,too-many-branches
            self, block: bool, timeout: Union[float, int, None]) -> any:
        """
        get element from queue

        Parameters
        ----------
        block : bool
            whether to block if queue is empty
        timeout : Union[float, int, None]
            maximum time to wait for an item if block is True; None means wait indefinitely

        Returns
        -------
        any
            the item retrieved from the queue

        Raises
        ------
        ShmQueueEmpty
            if the queue is empty and block is False
        """

        blocked = False
        deadline = (time.monotonic() + timeout) if (block and timeout is not None) else None

        if self.ref_data is None:
            raise ShmQueueNotInitializedError("shared memory has not been initialized or "\
                                              "already shut down.")

        while True: # exit event?

            if blocked:
                # make sure to sleep here so that other processes have the chance to
                # acquire the queue and put data
                time.sleep(BLOCK_SLEEP_TIME)

            lock_timeout = (max(0.0, deadline - time.monotonic())
                            if deadline is not None else timeout)
            with self._shm_lock.lock(timeout=lock_timeout):

                ref_data: tuple = self.ref_data.return_everything()
                put_pos = ref_data[1]
                get_pos = ref_data[2]
                buffer_size = ref_data[3]
                same_lap = ref_data[5]

                self.debug("Extracted ref data %s", ref_data)


                if get_pos == put_pos and same_lap == 1:
                    if not block:
                        raise ShmQueueEmpty("queue is empty")
                    if deadline is not None and time.monotonic() >= deadline:
                        raise ShmQueueEmpty(
                            f"get timed out: queue still empty after timeout {timeout}s")
                    self.debug("queue is empty")
                    blocked = True
                    continue


                get_pos_after_header = (get_pos + SIZE_HEADER) % buffer_size

                if get_pos_after_header <= get_pos:
                    # did a complete lap; if lap was the same it will not
                    # be different and vice versa
                    same_lap = int(not same_lap)
                    self.debug("read split header")
                    # extract payload length via slicing
                    header = ShmQueueHeader.from_bytes(
                        bytes(self.shm_payload.buf[get_pos:buffer_size]) + \
                        bytes(self.shm_payload.buf[0:get_pos_after_header])
                    )
                    data_size = header.payload_length

                else:
                    self.debug("getting data size from single header, "\
                            "get_pos: %s, get_pos_after_header: %s",
                            get_pos, get_pos_after_header)
                    # extract payload length
                    header = ShmQueueHeader.from_bytes(
                        self.shm_payload.buf[get_pos:get_pos_after_header]
                    )
                    data_size = header.payload_length


                self.debug("Extracted data size: %s", data_size)

                get_pos_after = (get_pos_after_header + data_size) % buffer_size
                data = None
                try:
                    # check if we have a complete lap
                    if get_pos_after <= get_pos:
                        if self.ref_data.same_lap == same_lap:
                            # prevent double
                            same_lap = int(not same_lap)
                        # NOTE expensive copying of data to concatenate it. However I did not
                        # find a easier way which is compatible with the current architecture.
                        # NOTE this might only affect certain serialization methods;
                        # for example msgpack supports feeding of data and thus does not
                        # require concatenation; however pickle does not support feeding and
                        # thus requires concatenation.
                        if get_pos_after_header < get_pos_after:
                            # it is possible that get_pos_after_header == 0 so that
                            # one skips a lap but
                            # get_pos_after_header is just 0 in which case you can use the normal
                            # concatenation
                            # covered with THIS version by mp async test
                            # get_pos= 4088
                            # SIZE_HEADER= 8
                            # buffer_size= 4096
                            # get_pos_after_header= 0 # <-- this is the problem
                            # get_pos_after = 12
                            return self._deserialize(header.serialization_method,
                                                    [self.shm_payload.buf[get_pos_after_header:\
                                                                        get_pos_after]])
                        # else:
                        # payload split over ring buffer, msgpack will use feeding, pickle will
                        # require concatenation of the data (copying that is)
                        return self._deserialize(header.serialization_method,
                                                    [self.shm_payload.buf[get_pos_after_header:\
                                                                        buffer_size],
                                                    self.shm_payload.buf[0:get_pos_after]])
                    # else:
                    return self._deserialize(header.serialization_method,
                                            [self.shm_payload.buf[get_pos_after_header:\
                                                                get_pos_after]])

                finally:
                    # update get position
                    self.ref_data.qsize -= 1
                    self.ref_data.get_pos = get_pos_after
                    self.ref_data.same_lap = same_lap

                    # NOTE
                    # data can be bytes or memoryview, depending if data were
                    #  decided by ring buffer approach


                    self.debug("New get position is %s",
                            get_pos_after)

                    if data is not None:
                        data = None
                        del data

    def release_lock(self):
        """
        release internal sync lock
        """
        self._shm_lock.release()

    def reduce_ref_counter(self):
        """
        reduce reference counter by one. this is useful if the shared memory is shared
        via inheritance (only possible on posix systems)

        However this is STRONGLY discouraged because it might lead to leaked memory
        """
        with self._shm_lock.lock():
            if self.ref_data is None:
                raise ShmQueueNotInitializedError("shared memory has not been initialized or " \
                                           "already closed.")
            if self.ref_data.ref_count == 0:
                raise ShmQueueRuntimeError("ref count is already 0 i.e. "\
                                           "it cannot be further reduced.")

            if self.ref_data.ref_count == 1:
                raise ShmQueueValueError("ref count is == 1 and you are trying to manually "\
                                         "reduce the ref count of the last reference. This "\
                                         "would result in dangling shared memory blocks. "\
                                         "Please call shutdown() instead to properly close "\
                                         "the shared memory blocks.")

            self.ref_data.ref_count -= 1
            self.debug("Reduced ref count to %s", self.ref_data.ref_count)

    def _serialize(self, serialization_method: int, data: any) -> bytes:
        """
        serialize data w.r.t. chose method

        Parameters
        ----------
        header : ShmQueueHeader
            header to be serialized
        data : bytes
            data to be serialized

        Returns
        -------
        bytes
            serialized header and data
        """
        if serialization_method > RESERVED_SERIALIZATION_METHOD_THRESHOLD:
            self.debug("Using custom serialization method %s", serialization_method)
            return self.custom_serialize(serialization_method, data)
        # else:
        try:
            if serialization_method == SerializationMethods.MSGPACK:
                # 0 (default) is reserved for msgpack
                try:
                    return msgpack.dumps(data)
                except TypeError as err:
                    # default method failed
                    self.warning("msgpack serialization failed with error %s.", err)
                    raise ShmQueueSerializationError from err
            elif serialization_method == SerializationMethods.PICKLE:
                if not self._pickle:
                    raise ShmQueueSerializationError("Pickle serialization is not enabled.")
                return pickle.dumps(data)
            # elif ... # for other serialization methods
            else:
                raise NotImplementedError(f"Serialization method {serialization_method} "\
                                            "is not implemented.")
        finally:
            self.debug("Serialized data with serialization method %s", serialization_method)

    def _deserialize(self, serialization_method: int, data: list[memoryview]) -> any:
        """
        deserialize data w.r.t. chose method

        Parameters
        ----------
        header : ShmQueueHeader
            header to be deserialized
        data : bytes
            data to be deserialized

        Returns
        -------
        any
            deserialized data
        """
        try:
            if serialization_method > RESERVED_SERIALIZATION_METHOD_THRESHOLD:
                self.debug("Using custom deserialization method %s", serialization_method)
                return self.custom_deserialize(serialization_method, data)
            # else:
            if serialization_method == SerializationMethods.MSGPACK:
                str_unpacker = msgpack.Unpacker(raw=False)
                try:
                    for item in data:
                        str_unpacker.feed(item)
                    return next(str_unpacker)
                except (UnicodeDecodeError, TypeError):
                    raw_unpacker = msgpack.Unpacker(raw=True)
                    for item in data:
                        raw_unpacker.feed(item)
                    return next(raw_unpacker)
            if len(data) > 1:
                # sady we have to concatenate the data
                data = b"".join(data)
            else:
                data = data[0]
            if serialization_method == SerializationMethods.PICKLE:
                if not self._pickle:
                    raise ShmQueueDeserializationError("Pickle deserialization is not enabled.")
                try:
                    return pickle.loads(data)
                except AttributeError as err:
                    msg = "pickle deserialization failed because the object to be "\
                            "deserialized is not defined in this process. "\
                            "Make sure the correct object type is properly imported. "\
                            f"Exact error: {err}"
                    self.error(msg)
                    raise ShmQueueDeserializationError(msg) from err
            # if ... # for other serialization methods

            # if not returned by this point, the serialization method is not implemented
            raise NotImplementedError(f"Deserialization method {serialization_method} "\
                                        "is not implemented.")
        finally:
            # make sure no option pointers to memoryview exist.
            # this prevents in interactive shells that close() fails after an exception
            # has been raised. can this be unit tested?
            data = None
            del data
            self.debug("Returned data from deserialization method %s", serialization_method)

    def custom_serialize(self, serialization_method: int, data: any) -> bytes:
        raise NotImplementedError("Custom serialization is not implemented yet. "\
                                  "Please use either msgpack or pickle serialization methods or "\
                                  "override this method.")

    def custom_deserialize(self, serialization_method: int, data: Union[bytes, memoryview]) -> any:
        raise NotImplementedError("Custom deserialization is not implemented yet. "\
                                  "Please use either msgpack or pickle serialization methods or "\
                                  "override this method.")

    def _create_or_attach_shm(self, name: str, size: int) -> tuple[ShmMode,
                                                                   shared_memory.SharedMemory]:
        """
        create or attach shared memory block

        Parameters
        ----------
        name : str
            name of the shared memory block
        size : int
            size of the shared memory block in bytes

        Returns
        -------
        tuple
            tuple of ShmMode and shared_memory.SharedMemory
        """
        if sys.version_info < (3, 13):
            try:
                return ShmMode.attach, shared_memory.SharedMemory(name=name)
            except FileNotFoundError:
                return ShmMode.create, shared_memory.SharedMemory(name=name,
                                                                  create=True,
                                                                  size=size)
        else:
            # from python 3.13 onwards the track parameter can be used to disable resource tracking
            try:
                # pylint: disable=(unexpected-keyword-arg)
                return ShmMode.attach,\
                    shared_memory.SharedMemory(name=name,
                                               track=self._track_resources)
            except FileNotFoundError:
                return ShmMode.create,\
                    shared_memory.SharedMemory(name=name,
                                               create=True,
                                               size=size,
                                               track=self._track_resources)
        raise ShmQueueRuntimeError(f"did not create or attach to shared memory block {name}")


    def qsize(self) -> int:
        """
        get current queue size

        Returns
        -------
        int
            current queue size
        """
        with self._shm_lock.lock():
            if self.ref_data is None:
                raise ShmQueueNotInitializedError("shared memory has not been initialized or "\
                                                  "already shut down.")
            return self.ref_data.qsize

    def max_buffer_size(self) -> int:
        """
        get maximum buffer size

        Returns
        -------
        int
            maximum size of the available shmqueue buffer
        """
        with self._shm_lock.lock():
            if self.ref_data is None:
                raise ShmQueueNotInitializedError("shared memory has not been initialized or "\
                                                  "already shut down.")
            return self.ref_data.buffer_size

    def buffer_occupancy(self) -> int:
        """
        get current buffer occupancy in bytes

        Returns
        -------
        int
            current buffer occupancy in bytes
        """
        with self._shm_lock.lock():
            if self.ref_data is None:
                raise ShmQueueNotInitializedError("shared memory has not been initialized or "\
                                                  "already shut down.")
            snap = self.ref_data.return_everything()
            # snap: ref_count, put_pos, get_pos, buffer_size, qsize, same_lap
            _, put_pos, get_pos, buffer_size, _, same_lap = snap
            if put_pos == get_pos and same_lap == 0:
                return buffer_size
            if put_pos < get_pos:
                return put_pos + buffer_size - get_pos
            return put_pos - get_pos

    def empty(self) -> bool:
        """
        check if the shared memory queue is empty

        Returns
        -------
        bool
            True if the shared memory queue is empty, False otherwise
        """
        with self._shm_lock.lock():
            if self.ref_data is None:
                raise ShmQueueNotInitializedError("shared memory has not been initialized or "\
                                                  "already shut down.")
            return self.ref_data.qsize == 0

    def ref_count(self) -> int:
        """
        get current reference count

        Returns
        -------
        int
            current reference count
        """
        with self._shm_lock.lock():
            if self.ref_data is None:
                raise ShmQueueNotInitializedError("shared memory has not been initialized or "\
                                                  "already shut down.")
            return self.ref_data.ref_count

    def debug_ref_data(self) -> ShmRefDataSnapshot:
        """
        Return a point-in-time snapshot of the shared-memory metadata fields.

        The snapshot is captured while holding the lock and is safe to read
        after the lock is released.

        Returns
        -------
        ShmRefDataSnapshot
            Named tuple with fields: ref_count, put_pos, get_pos,
            buffer_size, qsize, same_lap.
        """
        with self._shm_lock.lock():
            if self.ref_data is None:
                raise ShmQueueNotInitializedError("shared memory has not been initialized or "\
                                                  "already shut down.")
            return ShmRefDataSnapshot(*self.ref_data.return_everything())

    def full(self) -> bool:
        """
        check if the shared memory queue is full

        Returns
        -------
        bool
            True if the shared memory queue is full, False otherwise
        """
        # the buffer is full when put_pos and get_pos are in different laps (same_lap == 0)
        # and both positions are equal — meaning put has done one more full lap than get,
        # consuming all available buffer bytes
        with self._shm_lock.lock():
            if self.ref_data is None:
                raise ShmQueueNotInitializedError("shared memory has not been initialized or "\
                                                  "already shut down.")
            snap = self.ref_data.return_everything()
            # snap: ref_count, put_pos, get_pos, buffer_size, qsize, same_lap
            return snap[1] == snap[2] and snap[5] == 0

    def snapshot(self) -> ShmRefDataSnapshot:
        """
        Return a point-in-time snapshot of all metadata fields under a single lock.

        Use this instead of calling qsize(), empty(), full(), and buffer_occupancy()
        separately when you need multiple fields at once; each individual accessor
        acquires the lock independently.

        Returns
        -------
        ShmRefDataSnapshot
            Named tuple with fields: ref_count, put_pos, get_pos,
            buffer_size, qsize, same_lap.
        """
        with self._shm_lock.lock():
            if self.ref_data is None:
                raise ShmQueueNotInitializedError("shared memory has not been initialized or "\
                                                  "already shut down.")
            return ShmRefDataSnapshot(*self.ref_data.return_everything())
