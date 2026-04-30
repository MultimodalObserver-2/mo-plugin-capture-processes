import sys
import os
import time
import types
import unittest

SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

comm_mod    = types.ModuleType("mo.modules.communication")
comm_svc    = types.ModuleType("mo.modules.communication.services")
comm_ss     = types.ModuleType("mo.modules.communication.services.server_service")

class _ServerService:
    def send_capture_data(self, *a, **kw): pass
    def add_active_capture_plugin(self, *a, **kw): pass

comm_ss.ServerService = _ServerService
sys.modules.setdefault("mo.modules.communication", comm_mod)
sys.modules.setdefault("mo.modules.communication.services", comm_svc)
sys.modules.setdefault("mo.modules.communication.services.server_service", comm_ss)

import psutil
from processes_recorder.main import ProcessesCapturePlugin


class TestProcessesHardware(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.snapshot = ProcessesCapturePlugin.take_snapshot(time.time())

    def test_snapshot_returns_dict(self):
        self.assertIsInstance(self.snapshot, dict)

    def test_snapshot_has_timestamp(self):
        self.assertIn("captureTimestamp", self.snapshot)
        self.assertIsInstance(self.snapshot["captureTimestamp"], float)

    def test_snapshot_has_processes(self):
        self.assertIn("processes", self.snapshot)
        self.assertIsInstance(self.snapshot["processes"], list)

    def test_snapshot_has_processes(self):
        self.assertGreater(len(self.snapshot["processes"]), 0)

    def test_process_has_required_fields(self):
        proc = self.snapshot["processes"][0]
        for field in ("pid", "userName", "startInstant", "totalCpuDuration",
                      "command", "parentPid", "hasChildren", "supportsNormalTermination"):
            self.assertIn(field, proc)

    def test_process_pid_is_int(self):
        for proc in self.snapshot["processes"]:
            self.assertIsInstance(proc["pid"], int)

    def test_process_cpu_duration_nonnegative(self):
        for proc in self.snapshot["processes"]:
            self.assertGreaterEqual(proc["totalCpuDuration"], 0)

    def test_current_process_in_snapshot(self):
        own_pid = os.getpid()
        pids = {p["pid"] for p in self.snapshot["processes"]}
        self.assertIn(own_pid, pids)

    def test_csv_row_format(self):
        proc = self.snapshot["processes"][0]
        row = ProcessesCapturePlugin.proc_to_csv_row(proc, self.snapshot["captureTimestamp"])
        self.assertIsInstance(row, str)
        self.assertEqual(row.count(","), 8)

    def test_two_snapshots_different_timestamps(self):
        ts1 = time.time()
        time.sleep(0.05)
        ts2 = time.time()
        snap2 = ProcessesCapturePlugin.take_snapshot(ts2)
        self.assertGreater(snap2["captureTimestamp"], ts1)


if __name__ == '__main__':
    unittest.main()
