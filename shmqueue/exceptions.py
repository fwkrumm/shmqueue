"""
class for buffer management i.e. stores which position is next to write, next to read
etc.
"""

import queue


# empty queue
class ShmQueueEmpty(queue.Empty):
    pass

# queue full
class ShmQueueFull(queue.Full):
    pass

# general exceptions
class ShmQueueException(Exception):
    pass

# parameter error
class ShmQueueValueError(ValueError):
    pass

# type error
class ShmQueueTypeError(TypeError):
    pass

# buffer overflow error
class ShmQueueBufferOverflow(OverflowError):
    pass

# runtime error
class ShmQueueRuntimeError(RuntimeError):
    pass

# not yet initialized error
class ShmQueueNotInitializedError(RuntimeError):
    pass

# inbuild serialization error; try with alternative
class ShmQueueSerializationError(Exception):
    pass

class ShmQueueDeserializationError(Exception):
    pass
