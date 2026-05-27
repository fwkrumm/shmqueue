"""
main class of shared memory lock.
"""

import atexit
import gc
import logging
import mmap
import os
import signal
import tempfile
import weakref

if os.name == "nt":
    import win32api  # pylint: disable=import-error
    import win32con  # pylint: disable=import-error

# pylint: disable=wrong-import-position
from shmqueue.base_logger import ShmModuleBaseLogger
from shmqueue.shared_memory_collection import ShmCollection, SIZE_HEADER, SerializationMethods, \
    DebugRefLogAction
from shmqueue import exceptions
from shmqueue.exceptions import ShmQueueValueError, ShmQueueTypeError
# pylint: enable=wrong-import-position

__all__ = [
    "ShmQueue",
    "SYSTEM_PAGESIZE",
    "SIZE_HEADER",
    "exceptions",
    "SerializationMethods",
    "DebugRefLogAction",
    ]


SYSTEM_PAGESIZE = mmap.PAGESIZE # default if not specified


class ShmQueue(ShmModuleBaseLogger):

    """
    lock class using shared memory to synchronize shared resources access
    """

    def __init__(self,
                 queue_name: str,
                 *,
                 logger: logging.Logger = None,
                 buffer_size: int = SYSTEM_PAGESIZE,
                 debug_ref_to_file_log: bool = False,
                 allow_pickle: bool = True,
                 track_resources: bool = False
                 ):
        """
        default init. set shared memory name (for lock) and poll_interval.
        the latter is used to check if lock is available every poll_interval seconds

        Parameters
        ----------
        queue_name : str
            name of the underlying shared memory block of the queue
        logger : logging.Logger
            logger object
        buffer_size : int
            size of the buffer chunk in bytes; has to be a multiple of system pagesize because
            if a process attaches to shared memory, it will allocate multiples of the system
            pagesize.
        debug_ref_to_file_log : bool
            if True, the references will be logged to a file for debugging purposes.
            This is useful to track down memory leaks and reference counting issues.
            Default is False, i.e. no logging of reference count to file.
        allow_pickle : bool
            if True, the queue will use pickle to serialize objects that are not
            serializable by msgpack. If False, only msgpack will be used and objects that are not
            serializable by msgpack will raise an exception.
            Default is True, i.e. pickle is allowed.
        track_resources : bool
            if True, Python's resource tracker is enabled for the underlying shared-memory blocks.
            This may produce false-positive warnings on POSIX (Python < 3.13) when the last
            process releases the segment.  Default is False (warnings suppressed).
        """
        # base logger
        super().__init__(logger=logger)
        self._debug_ref_to_file_log = None  # declare early to not run into attribute errors
        self._finalizer_registered = False  # tracks whether weakref.finalize was set up

        # type checks
        if not isinstance(queue_name, str):
            raise ShmQueueTypeError("queue_name must be a string")

        if not isinstance(buffer_size, int):
            raise ShmQueueTypeError("buffer_size must be an integer")

        if buffer_size < 0:
            raise ShmQueueValueError("buffer_size must be a positive integer")

        # value checks
        if buffer_size % SYSTEM_PAGESIZE != 0:
            buffer_size = (buffer_size // SYSTEM_PAGESIZE + 1) * SYSTEM_PAGESIZE
            # buffer_size // SYSTEM_PAGESIZE gives the number of system pages that fit into the
            # buffer, adding 1 assures that we round up to the next page and also never have a
            # zero-sized buffer. multiplying by SYSTEM_PAGESIZE gives the size of the buffer in
            # bytes. this is done to ensure that the buffer size is a multiple of the system
            # pagesize
            self.warning("buffer_size is not a multiple of the system pagesize %s. It will be "\
                         "rounded up to the next multiple of the system pagesize.", SYSTEM_PAGESIZE)
            assert buffer_size % SYSTEM_PAGESIZE == 0, "buffer_size is not a multiple "\
                "of the system pagesize. This must not happen."
        if buffer_size < SYSTEM_PAGESIZE:
            raise ShmQueueValueError("buffer_size must be a multiple of the system "\
                                     f"pagesize. System pagesize is {SYSTEM_PAGESIZE} bytes. "\
                                     "You can check it programmatically via: "\
                                     "from shmqueue.shmqueue_main import SYSTEM_PAGESIZE")

        self._shm_collection = ShmCollection(queue_name,
                                             buffer_size,
                                             allow_pickle,
                                             track_resources=track_resources,
                                             logger=logger)

        # since the shared memory is  unambiguously identified by the queue name we can use
        # it as identifier
        self._identifier = queue_name

        self._debug_ref_to_file_log = debug_ref_to_file_log

        if self._debug_ref_to_file_log:
            # if debug_ref_to_file_log is True, we log the reference count to a file
            self._debug_ref_log(DebugRefLogAction.CREATE)

        self.debug("created shared memory queue %s", self)

    def _debug_ref_log(self, action: str, path: str = tempfile.gettempdir()):
        """
        debug function to log the reference count to a file

        note that e.g.

        import shmqueue;s2 = shmqueue.ShmQueue("asd", debug_ref_to_file_log=True)

        this prints ref count two because the queue is created BEFORE s2 is garbage collected.
        """
        path = os.path.abspath(os.path.join(path, "shmqueue", f"_ref_log_{self._identifier}.json"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.debug("Will log reference info to file %s", path)
        self._shm_collection.debug_ref_log(path, action)

    def __repr__(self):
        return f"ShmQueue(queue_name={self._identifier}, buffer_size={self.max_buffer_size()})"

    def set_console_handlers(self, register_atexit: bool = False):
        """
        adding console handlers to ensure that the shared memory queue is properly
        closed when the process; this should be used as fallback if there are
        problems with releasing the shared memory resources. usually however
        this should not be necessary

        Parameters
        ----------
        register_atexit : bool, optional
            Indicates if the atexit handler should be registered, by default False

        Returns
        -------
        None
            This function does not return anything
        """

        if os.name == "posix":
            def clean_up(signum, frame): # pylint: disable=unused-argument
                """
                cleanup function to close the shared memory queue
                """
                self.shutdown(use_gc=True)
            signal.signal(signal.SIGINT, clean_up)
            signal.signal(signal.SIGTERM, clean_up)
            if hasattr(signal, "SIGHUP"): # not available on windows
                signal.signal(signal.SIGHUP, clean_up)  # pylint: disable=no-member


        if os.name == "nt":

            def console_handler(ctrl_type):
                if ctrl_type in (win32con.CTRL_C_EVENT,
                                 win32con.CTRL_CLOSE_EVENT,
                                 win32con.CTRL_LOGOFF_EVENT,
                                 win32con.CTRL_SHUTDOWN_EVENT,):
                    self.shutdown(use_gc=True)
                    return True  # Prevent immediate termination if possible
                return False  # Continue default behavior

            win32api.SetConsoleCtrlHandler(console_handler, True)  # pylint: disable=c-extension-no-member

        weakref.finalize(self, self.shutdown, use_gc=True)
        self._finalizer_registered = True

        if register_atexit:
            # register atexit handler to close the shared memory queue
            # usually this should not be necessary since the usage of signal and weakref, but
            # safe is safe
            atexit.register(self.shutdown, use_gc=True)

    def shutdown(self, use_gc: bool = True):
        """
        close the shared memory queue

        TODO complete debug log stuff should be moved to shutdown of shm collection
        since otherwise we would introduce redundant code here.
        even this debug ref log required an additional acquireent of the lock which we
        do in the release shutdown function either way. same holds true for init step;
        long story short currently the debug ref log requires an additional lock acquirement.
        However since this is only a debug function and not for production its not critical.

        TODO if s2.shutdown() is called but NOT garbage collection is performed then
        it is possible that at del s2 there is an error because ref shared memory is None
        (runtimerrror). as said, this should be partly resolved if the magic is within
        shm collection class. HOWEVER, the file should be more flexible, especially if
        shutdown is already within the json it doesnt matter if shm is none
        """
        self.debug("closing shared memory queue")
        if self._debug_ref_to_file_log:
            self._debug_ref_log(DebugRefLogAction.SHUTDOWN)
        try:
            if self._shm_collection.pid != os.getpid():
                # we will not raise exception since releasing shared memory might still work
                self.error("close called from different process. This means the class "\
                           "has been shared via inheritance (should be only possible on "\
                           "posix systems). This means that the ref count will be "\
                           "incorrect since add_ref_manually has not been used. Releasing "\
                           "memory will be tried anyway but leaked resources might occur.")
            self._shm_collection.shutdown()
        except AttributeError:
            # if program terminates before collection object is created
            pass
        except RuntimeError:
            self.error("RuntimeError at closing shm collection object. This might happen if the "\
                       "process has been interrupted via KeyboardInterrupt or termination.")
            self._shm_collection.release_lock()
            # in that case should be force cleared!
            self._shm_collection.shutdown()
        if use_gc is True:
            self.debug("forcing garbage collection to delete resources")
            # force garbage collection to release shared memory resources
            # this is necessary because the shared memory is not released automatically
            # if the process is terminated
            gc.collect()  # force garbage collection to release shared memory resources

    def clear(self):
        """
        clear the shared memory queue
        """
        self._shm_collection.clear()

    def add_ref_manually(self, pid: int):
        """
        add reference count manually for given process

        NOTE this is STRONGLY discouraged to use this method. It is only provided as fallback.
        Usually the user should prevent the shared memory from being shared via inheritance.

        Parameters
        ----------
        pid : int
            process id of the process which should increment the reference count
        """
        if os.name != "posix":
            self.warning("This usually should only be necessary on posix systems.")
        if self._shm_collection.pid == pid:
            self.warning("The pid %s of the process which should increment the "\
                         "reference count is the same as the pid of the process "\
                         "i.e. the reference should already be counted!", pid)
        self._shm_collection.add_ref_manually(pid)

    def __del__(self):
        # If set_console_handlers() was called, weakref.finalize already handles cleanup;
        # avoid calling shutdown() twice.
        if not getattr(self, "_finalizer_registered", False):
            self.shutdown()

    def qsize(self) -> int:
        return self._shm_collection.qsize()

    def max_buffer_size(self) -> int:
        """
        get the maximum size of the shared memory queue

        Returns
        -------
        int
            maximum size of the available shmqueue buffer
        """
        return self._shm_collection.max_buffer_size()

    def buffer_occupancy(self) -> int:
        """
        get the current buffer occupancy of the shared memory queue

        Returns
        -------
        int
            current buffer occupancy in bytes
        """
        return self._shm_collection.buffer_occupancy()

    def empty(self) -> bool:
        """
        check if the shared memory queue is empty

        Returns
        -------
        bool
            True if the shared memory queue is empty, False otherwise
        """
        return self._shm_collection.empty()

    def full(self) -> bool:
        """
        check if the shared memory queue is full (zero bytes free in the ring buffer)

        Returns
        -------
        bool
            True if the shared memory queue is full, False otherwise
        """
        return self._shm_collection.full()

    def put(self,
            data:any,
            block=True,
            timeout=None,
            serialization_method: SerializationMethods = SerializationMethods.DEFAULT):
        """
        put data into the shared memory queue

        Parameters
        ----------
        data : any
            data to put into the shared memory queue
        """
        self._shm_collection.put(data, block, timeout, serialization_method)

    def custom_serialize(self, callable_function):
        """
        set a custom serialization method for the shared memory queue

        Parameters
        ----------
        callable : function
            function to use for serialization. It should take one argument and return a serialized
            object. The object will be deserialized when getting from the queue.
        """
        self._shm_collection.custom_serialize = callable_function

    def custom_deserialize(self, callable_function):
        """
        set a custom deserialization method for the shared memory queue

        Parameters
        ----------
        callable : function
            function to use for deserialization. It should take one argument and return a
            deserialized object. The object will be serialized when putting into the queue.
        """
        self._shm_collection.custom_deserialize = callable_function

    def get(self, block=True, timeout=None) -> any:
        """
        get data from the shared memory queue

        Returns
        -------
        any
            data from the shared memory queue
        """
        return self._shm_collection.get(block, timeout)

    def reduce_ref_counter(self):
        """
        reduce the reference counter of the shared memory queue by one

        TODO this should only be allowed if the master queue allows it.
             so I need to add some "settings" shared memory block
             (can be part of ref data I guess?)
        """
        self._shm_collection.reduce_ref_counter()

    @property
    def refs(self):
        return self._shm_collection.ref_count()

    @property
    def identifier(self):
        return self._identifier

    @property
    def debug_ref_data(self):
        """
        Return a point-in-time snapshot of the shared-memory metadata.

        Returns
        -------
        ShmRefDataSnapshot
            Named tuple with fields: ref_count, put_pos, get_pos,
            buffer_size, qsize, same_lap.
        """
        return self._shm_collection.debug_ref_data()

    def snapshot(self):
        """
        Return a point-in-time snapshot of all metadata fields under a single lock.

        Use this instead of calling qsize(), empty(), full(), and buffer_occupancy()
        separately when multiple fields are needed at once.

        Returns
        -------
        ShmRefDataSnapshot
            Named tuple with fields: ref_count, put_pos, get_pos,
            buffer_size, qsize, same_lap.
        """
        return self._shm_collection.snapshot()
