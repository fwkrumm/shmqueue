# shmqueue — Agent Context

## Purpose
IPC queue via POSIX/Windows shared-mem ring-buffer. Deps: `shmlock` (mutex), `msgpack` (serial), `pickle` (fallback).

## Modules
```
shmqueue/
  __init__.py                   re-exports ShmQueue, SYSTEM_PAGESIZE, SIZE_HEADER, exceptions, SerializationMethods
  shmqueue_main.py              ShmQueue — public API
  shared_memory_collection.py  ShmCollection — ring-buffer, put/get, serialization
  shared_memory_data.py         ShmRefData — metadata block in shm
  buffer_operations.py          ShmQueueHeader, SIZE_HEADER, SerializationMethods
  exceptions.py                 project exceptions
  base_logger.py                logging mixin
```

## Class Hierarchy
```
ShmQueue              (shmqueue_main.py)
  └─ ShmCollection    (shared_memory_collection.py)
       ├─ ShmRefData  (shared_memory_data.py)
       ├─ ShmQueueHeader (buffer_operations.py)
       └─ shmlock.ShmLock
```

## Key Constants
| Symbol | Value | Defined in |
|--------|-------|-----------|
| `SIZE_HEADER` | 8 bytes (2 × uint32) | `buffer_operations.py` |
| `SYSTEM_PAGESIZE` | `mmap.PAGESIZE` (typ. 4096) | `shmqueue_main.py`, `shared_memory_collection.py` |
| `BLOCK_SLEEP_TIME` | 0.01 s | `shared_memory_collection.py` |

## Ring Buffer
- `buffer_size` → multiple of `SYSTEM_PAGESIZE`.
- Item size = `SIZE_HEADER + len(msgpack.packb(payload))`. Ex: `b"dummy data"` (10B raw) → bin8=12B → **20B** total.
- `ShmRefData`: `ref_count`, `put_pos`, `get_pos`, `buffer_size`, `qsize`, `same_lap`.
- `same_lap==1` → same lap (empty when `put_pos==get_pos`). `same_lap==0` + equal pos → full.

## Serialization
- Default: **msgpack** (`SerializationMethods.MSGPACK=0`).
- Fallback: **pickle** (on `TypeError`; disable via `allow_pickle=False`).
- Custom IDs ≤ 1000 reserved (`RESERVED_SERIALIZATION_METHOD_THRESHOLD`).
- Wire: `bytes` of len N → `N+2` bytes (msgpack bin8).

## Exceptions (`exceptions.py`)
| Exception | Superclass | When |
|-----------|-----------|------|
| `ShmQueueFull` | `queue.Full` | `put(block=False)` on full buffer |
| `ShmQueueEmpty` | `queue.Empty` | `get(block=False)` on empty buffer |
| `ShmQueueValueError` | `ValueError` | bad param values |
| `ShmQueueTypeError` | `TypeError` | wrong param types |
| `ShmQueueRuntimeError` | `RuntimeError` | state inconsistency |
| `ShmQueueSerializationError` | `Exception` | serial fail |
| `ShmQueueDeserializationError` | `Exception` | deserial fail |

## Platform
- **Win**: `pywin32` (`win32api`, `win32con`) → signal handling.
- **POSIX**: `remove_shm_from_resource_tracker` suppresses false warnings (py<3.13); py≥3.13 uses `track=` param directly.
- `ref_count` in `ShmRefData`; last closer calls `shm.unlink()`.

## Tests
| File | Coverage |
|------|----------|
| `tests/test_basics.py` | single-proc put/get, serial types, edge cases |
| `tests/test_multiprocessing.py` | multi-proc async puts (overflow) + sequential ordered puts |
| `tests/test_serialization.py` | serial/deserial round-trips |

### test_multiprocessing invariants
- `BUFFER_SIZE = SYSTEM_PAGESIZE` (1 page).
- `ASYNC_TEST_DATA = b"dummy data"` → item=**20B** (`SIZE_HEADER=8` + msgpack 12B).
- Guards: `BUFFER_SIZE >= item_size` (fits≥1); `RUNS_ASYNC*item_size > BUFFER_SIZE` (overflows → exercises `ShmQueueFull`).
