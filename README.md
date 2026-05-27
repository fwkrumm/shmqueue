# shmqueue

## Table of Contents

- [About](#about)
- [When to Use](#when-to-use)
- [Installation](#installation)
- [Quick Dive](#quick-dive)
- [Serialization](#serialization)
- [Troubleshooting and Known Issues](#troubleshooting-and-known-issues)
- [ToDos](#todos)
- [Release History](#release-history)

---
<a name="about"></a>
<a id="about"></a>

## About

This module is currently under development and may undergo frequent changes on the master branch. It might not be free of bugs.

**Note:** The core architecture and initial implementation were written by hand. Due to time constraints, parts of the core code and the unit tests were completed with AI assistance (GitHub Copilot / Claude).

`shmqueue` is an inter-process FIFO queue backed by a shared-memory ring buffer. Multiple processes can `put` and `get` arbitrary Python objects without passing queue objects between processes — only the queue name is required. Under the hood it uses [`shmlock`](https://github.com/fwkrumm/shmlock) for synchronization and `msgpack` for serialization (with an optional `pickle` fallback).

---
<a name="when-to-use"></a>
<a id="when-to-use"></a>

## When to Use

**Good fit:**
- IPC between unrelated processes on the same machine without a broker (no Redis, ZMQ, pipes).
- Low-to-moderate throughput where a 10 ms poll interval is acceptable.
- Heterogeneous payloads — `msgpack` handles most primitives; `pickle` covers arbitrary Python objects.

**Not a good fit:**
- Very high throughput / low latency (the lock uses a polling interval).
- Cross-machine communication.
- Situations where `pickle` deserialization from untrusted data is a security concern — use `allow_pickle=False` and msgpack-compatible types only.

**Notable mention:** If performance is the primary concern, [shaneyuee/shmqueue](https://github.com/shaneyuee/shmqueue) is worth a look — it is a C-based, lock-free shared-memory queue. I did not test it myself though.

---
<a name="installation"></a>
<a id="installation"></a>

## Installation

```
git clone <repo-url>
cd shmqueue
pip install -r requirements.txt
pip install .
```

On Windows, optionally install `pywin32` for console signal handling (`set_console_handlers()`).

---
<a name="quick-dive"></a>
<a id="quick-dive"></a>

## Quick Dive

```python
import shmqueue

# ── Producer process ──────────────────────────────────────────────────────────
q = shmqueue.ShmQueue("my_queue", buffer_size=shmqueue.SYSTEM_PAGESIZE)
q.put({"hello": "world"})
q.put(b"raw bytes")
q.put(42)

# ── Consumer process (same name, any process on the same machine) ─────────────
q = shmqueue.ShmQueue("my_queue")
item = q.get()           # blocks until data is available
item = q.get(block=False)  # raises ShmQueueEmpty if nothing there

# ── Non-blocking put ──────────────────────────────────────────────────────────
try:
    q.put("data", block=False)
except shmqueue.exceptions.ShmQueueFull:
    pass

# ── Timeout ───────────────────────────────────────────────────────────────────
item = q.get(block=True, timeout=2.0)   # raises ShmQueueEmpty after 2 s

# ── Status ────────────────────────────────────────────────────────────────────
q.qsize()            # number of items currently in queue
q.empty()            # True / False
q.full()             # True / False
q.buffer_occupancy() # bytes used in ring buffer
snap = q.snapshot()  # all fields in one lock acquisition (ShmRefDataSnapshot)

# ── Cleanup ───────────────────────────────────────────────────────────────────
q.shutdown()         # decrement ref count; last closer frees shared memory
```

### Multi-process example

```python
import multiprocessing, shmqueue, time

QUEUE_NAME = "demo_queue"

def producer():
    q = shmqueue.ShmQueue(QUEUE_NAME)
    for i in range(10):
        q.put(i)
    q.shutdown()

def consumer():
    q = shmqueue.ShmQueue(QUEUE_NAME)
    received = []
    while len(received) < 10:
        try:
            received.append(q.get(block=True, timeout=5.0))
        except shmqueue.exceptions.ShmQueueEmpty:
            break
    print(received)
    q.shutdown()

if __name__ == "__main__":
    q_main = shmqueue.ShmQueue(QUEUE_NAME)   # keep shm alive for duration
    p = multiprocessing.Process(target=producer)
    c = multiprocessing.Process(target=consumer)
    p.start(); c.start()
    p.join();  c.join()
    q_main.shutdown()
```

### Optional: register exit handlers

```python
q = shmqueue.ShmQueue("my_queue")
# Registers signal, atexit, and weakref handlers so shm is released on
# unexpected termination (SIGINT / SIGTERM / console close on Windows).
q.set_console_handlers(register_atexit=True)
```

---
<a name="serialization"></a>
<a id="serialization"></a>

## Serialization

| Method | ID | Notes |
|--------|----|-------|
| `msgpack` | 0 (default) | Fast; supports most primitives and `bytes`. |
| `pickle` | 1 | Fallback for arbitrary Python objects. Disable with `allow_pickle=False`. |
| Custom | ≥ 1001 | Override `custom_serialize` / `custom_deserialize` on `ShmCollection`. |

```python
# Disable pickle:
q = shmqueue.ShmQueue("my_queue", allow_pickle=False)

# Explicit serialization method per put. If picke is disabled, this will raise ShmQueueSerializationError:
q.put("test", serialization_method=shmqueue.SerializationMethods.PICKLE)
```

### Custom serialization

Any method ID ≥ `SerializationMethods.FIRST_PUBLIC_NUMBER + 1` is user-defined. Provide a serializer and deserializer:

```python
import struct, shmqueue
from shmqueue import SerializationMethods

MY_METHOD = SerializationMethods.FIRST_PUBLIC_NUMBER + 1

q = shmqueue.ShmQueue("my_queue", buffer_size=1000)

def my_serializer(method: int, data) -> bytes:
    if method == MY_METHOD:
        return struct.pack("I", data)
    raise NotImplementedError

def my_deserializer(method: int, chunks: list) -> int:
    if method == MY_METHOD:
        return struct.unpack("I", b"".join(chunks))[0]
    raise NotImplementedError

q.custom_serialize(my_serializer)
q.custom_deserialize(my_deserializer)

q.put(42, serialization_method=MY_METHOD)
assert q.get() == 42
```

---
<a name="troubleshooting-and-known-issues"></a>
<a id="troubleshooting-and-known-issues"></a>

## Troubleshooting and Known Issues

### Resource Tracking (POSIX, Python < 3.13)

Python's `resource_tracker` may emit false-positive warnings about leaked shared-memory segments when multiple processes share the same block. `shmqueue` suppresses these warnings by default (`track_resources=False`). To opt back in:

```python
q = shmqueue.ShmQueue("my_queue", track_resources=True)
```

### Buffer size

`buffer_size` is rounded up to the nearest multiple of `SYSTEM_PAGESIZE` (typically 4096 bytes). A single item occupies `SIZE_HEADER (8 B) + len(msgpack.packb(payload))` bytes. An item larger than `buffer_size` raises `ShmQueueValueError` immediately.

### Abrupt process termination

If a process is killed during a `put`/`get`, the internal `shmlock` mutex may remain locked. Call `set_console_handlers()` and/or `register_atexit=True` to mitigate this. See the [`shmlock` README](https://github.com/fwkrumm/shmlock) for details on the underlying lock behavior.


---
<a name="release-history"></a>
<a id="release-history"></a>

## Release History

### 0.0.2
- Fix README anchors for correct PyPI rendering

### 0.0.1
- Initial release


---
<a name="todos"></a>
<a id="todos"></a>

## ToDos

TODO
- improve debug file json handling (see inline comments in `shmqueue_main.py`)
- add put_nowait/get_nowait methods
