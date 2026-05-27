"""
Tests for the DebugRefLogAction enum and the ShmCollection.debug_ref_log() method.
"""
import json
import os
import tempfile
import unittest
import shmqueue
from shmqueue.exceptions import ShmQueueNotInitializedError, ShmQueueValueError
from shmqueue.shared_memory_collection import DebugRefLogAction

QUEUE_NAME_BASE = "drl"
BUFFER_SIZE = shmqueue.SYSTEM_PAGESIZE


def _make_queue(suffix):
    return shmqueue.ShmQueue(f"{QUEUE_NAME_BASE}_{suffix}", buffer_size=BUFFER_SIZE)


def _log_path(name):
    return os.path.join(tempfile.gettempdir(), "shmqueue", f"_ref_log_{name}.json")


class TestDebugRefLogActionEnum(unittest.TestCase):
    """Unit tests for the DebugRefLogAction enum itself."""

    def test_members_exist(self):
        self.assertIs(DebugRefLogAction.CREATE, DebugRefLogAction("create"))
        self.assertIs(DebugRefLogAction.SHUTDOWN, DebugRefLogAction("shutdown"))

    def test_values(self):
        self.assertEqual(DebugRefLogAction.CREATE.value, "create")
        self.assertEqual(DebugRefLogAction.SHUTDOWN.value, "shutdown")

    def test_exactly_two_members(self):
        self.assertEqual(len(DebugRefLogAction), 2)


class TestDebugRefLog(unittest.TestCase):
    """Tests for ShmCollection.debug_ref_log() with DebugRefLogAction."""

    def test_invalid_action_type_raises_value_error(self):
        """Passing a plain string instead of the enum raises ShmQueueValueError."""
        q = _make_queue("invalid_action")
        path = _log_path(f"{QUEUE_NAME_BASE}_invalid_action")
        with self.assertRaises(ShmQueueValueError):
            q._shm_collection.debug_ref_log(path, "create")

    def test_create_writes_created_and_ref_count(self):
        """CREATE action writes 'created' timestamp and ref_count to the JSON file."""
        q = _make_queue("create_writes")
        path = _log_path(f"{QUEUE_NAME_BASE}_create_writes")
        os.makedirs(os.path.dirname(path), exist_ok=True)

        q._shm_collection.debug_ref_log(path, DebugRefLogAction.CREATE)

        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        uuid = q._shm_collection._shm_lock.uuid
        self.assertIn(uuid, data)
        self.assertIn("created", data[uuid])
        self.assertIn("ref_count", data[uuid])

    def test_shutdown_adds_shutdown_timestamp(self):
        """SHUTDOWN action adds a 'shutdown' key to an existing CREATE entry."""
        q = _make_queue("shutdown_writes")
        path = _log_path(f"{QUEUE_NAME_BASE}_shutdown_writes")
        os.makedirs(os.path.dirname(path), exist_ok=True)

        q._shm_collection.debug_ref_log(path, DebugRefLogAction.CREATE)
        q._shm_collection.debug_ref_log(path, DebugRefLogAction.SHUTDOWN)

        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        uuid = q._shm_collection._shm_lock.uuid
        self.assertIn("shutdown", data[uuid])

    def test_create_on_shutdown_queue_raises_not_initialized(self):
        """CREATE after the queue is shut down raises ShmQueueNotInitializedError."""
        q = _make_queue("create_after_shutdown")
        col = q._shm_collection
        path = _log_path(f"{QUEUE_NAME_BASE}_create_after_shutdown")
        q.shutdown()
        with self.assertRaises(ShmQueueNotInitializedError):
            col.debug_ref_log(path, DebugRefLogAction.CREATE)

    def test_end_to_end_via_debug_ref_to_file_log(self):
        """ShmQueue(debug_ref_to_file_log=True) uses both CREATE and SHUTDOWN without raising."""
        q = shmqueue.ShmQueue(f"{QUEUE_NAME_BASE}_e2e",
                              buffer_size=BUFFER_SIZE,
                              debug_ref_to_file_log=True)
        q.shutdown()  # exercises SHUTDOWN enum path; must not raise


if __name__ == "__main__":
    unittest.main(verbosity=2)
