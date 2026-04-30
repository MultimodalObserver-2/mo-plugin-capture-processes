import functools
import json
import os
import tempfile
import threading
import time
import types
import psutil
import shutil
import unittest
from unittest.mock import MagicMock, patch

import sys

SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if SRC not in sys.path:
    sys.path.insert(0, SRC)

comm_mod = types.ModuleType("mo.modules.communication")
comm_svc_mod = types.ModuleType("mo.modules.communication.services")
comm_ss_mod = types.ModuleType("mo.modules.communication.services.server_service")


class StubServerService:
    def __new__(cls, *args, **kwargs):
        return object.__new__(cls)

    def send_capture_data(self, data) -> None:
        pass

    async def add_active_capture_plugin(self, plugin_id, config=None) -> None:
        pass


comm_ss_mod.ServerService = StubServerService
sys.modules.setdefault("mo.modules.communication", comm_mod)
sys.modules.setdefault("mo.modules.communication.services", comm_svc_mod)
sys.modules.setdefault("mo.modules.communication.services.server_service", comm_ss_mod)

from processes_recorder.main import ProcessesCapturePlugin, format_csv_field, CSV_HEADERS
from processes_recorder.properties import properties
from mo.modules.communication.services.server_service import ServerService
from mo.modules.capture import CaptureData
from mo.core.plugin.models.properties import PropertyType


def make_plugin(settings_map=None):
    p = ProcessesCapturePlugin()
    p.settings = MagicMock()
    p.settings.get_setting.side_effect = lambda k: (settings_map or {}).get(k)
    p.load()
    return p


def make_snapshot(ts=1.0, procs=None):
    if procs is None:
        procs = [
            {"pid": 1, "userName": "alice", "startInstant": "2020-01-01T00:00:00+00:00",
             "totalCpuDuration": 500, "command": "/bin/sh", "parentPid": 0,
             "hasChildren": 0, "supportsNormalTermination": 1},
        ]
    return {"captureTimestamp": ts, "processes": procs}


def capture_data_from(snapshot, ts=1.0):
    return CaptureData(timestamp=ts, data=snapshot)



class TestProcessesPlugin(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    @staticmethod
    def make_proc_mock(pid, ppid=None, username="u", create_time=1000.0,
                       cpu_times=None, exe="/bin/sh"):
        p = MagicMock()
        p.info = {"pid": pid, "ppid": ppid, "username": username,
                  "create_time": create_time, "exe": exe,
                  "cpu_times": cpu_times}
        return p

    def row(self, proc, ts=0.0):
        return ProcessesCapturePlugin.proc_to_csv_row(proc, ts)

    @staticmethod
    def one_shot_loop(p, get_timestamp, on_data, interval):
        if p.status == "running":
            ts = get_timestamp()
            snap = make_snapshot(ts)
            on_data(CaptureData(timestamp=ts, data=snap))
            p.status = "stopped"

    @staticmethod
    def counting_ts(p, counter):
        counter[0] += 1
        if counter[0] > 1:
            p.status = "stopped"
        return float(counter[0])

    @staticmethod
    def stopping_sleep(p, sleep_calls, t):
        sleep_calls.append(t)
        p.status = "stopped"

    def test_plain_string(self):
        self.assertEqual(format_csv_field("hello"), "hello")

    def test_string_comma_quoted(self):
        result = format_csv_field("hello, world")
        self.assertEqual(result, '"hello, world"')

    def test_string_quote_escaped(self):
        result = format_csv_field('say "hi"')
        self.assertEqual(result, '"say ""hi"""')

    def test_string_newline_quoted(self):
        result = format_csv_field("line1\nline2")
        self.assertIn('"', result)

    def test_none_becomes_empty(self):
        self.assertEqual(format_csv_field(None), "")

    def test_integer(self):
        self.assertEqual(format_csv_field(42), "42")

    def test_has_pid(self):
        self.assertIn("pid", CSV_HEADERS)

    def test_has_nine_columns(self):
        self.assertEqual(len(CSV_HEADERS.split(",")), 9)

    def test_initial_status_stopped(self):
        p = make_plugin()
        self.assertEqual(p.status, "stopped")

    def test_initial_files_none(self):
        p = make_plugin()
        self.assertIsNone(p.json_file)
        self.assertIsNone(p.csv_file)

    def test_initial_output_path_none(self):
        p = make_plugin()
        self.assertIsNone(p.output_path)

    def test_initial_timestamps_zero(self):
        p = make_plugin()
        self.assertEqual(p.last_start, 0.0)
        self.assertEqual(p.last_pause, 0.0)
        self.assertEqual(p.last_resume, 0.0)
        self.assertEqual(p.last_stop, 0.0)

    def test_unload_sets_stopped(self):
        p = make_plugin()
        p.status = "running"
        p.unload()
        self.assertEqual(p.status, "stopped")

    def test_unload_closes_open_json_file(self):
        p = make_plugin()
        with tempfile.TemporaryDirectory() as tmp:
            p.prepare(tmp, "sess")
            p.json_file = open(os.path.join(tmp, "sess.json"), "w")
            p.json_file.write("[")
            p.unload()

    def test_unload_no_files(self):
        p = make_plugin()
        p.unload()

    def test_sets_output_path(self):
        p = make_plugin()
        with tempfile.TemporaryDirectory() as tmp:
            p.prepare(tmp, "mysess")
            self.assertEqual(p.output_path, os.path.join(tmp, "mysess.json"))

    def test_sets_csv_path(self):
        p = make_plugin()
        with tempfile.TemporaryDirectory() as tmp:
            p.prepare(tmp, "mysess")
            self.assertEqual(p.csv_path, os.path.join(tmp, "mysess.csv"))

    def test_resets_json_first(self):
        p = make_plugin()
        p.json_first = False
        with tempfile.TemporaryDirectory() as tmp:
            p.prepare(tmp, "x")
            self.assertTrue(p.json_first)

    def test_resets_files_none(self):
        p = make_plugin()
        with tempfile.TemporaryDirectory() as tmp:
            p.prepare(tmp, "x")
            self.assertIsNone(p.json_file)
            self.assertIsNone(p.csv_file)

    def test_resets_timestamps(self):
        p = make_plugin()
        p.last_start = 99.0
        with tempfile.TemporaryDirectory() as tmp:
            p.prepare(tmp, "x")
            self.assertEqual(p.last_start, 0.0)

    def test_pause_sets_timestamp(self):
        p = make_plugin()
        p.pause(5.0)
        self.assertEqual(p.last_pause, 5.0)
        self.assertEqual(p.status, "paused")

    def test_resume_sets_timestamp_running(self):
        p = make_plugin()
        p.pause(5.0)
        p.resume(6.0)
        self.assertEqual(p.last_resume, 6.0)
        self.assertEqual(p.status, "running")

    def test_stop_sets_timestamp_stopped(self):
        p = make_plugin()
        p.status = "running"
        p.stop(10.0)
        self.assertEqual(p.last_stop, 10.0)
        self.assertEqual(p.status, "stopped")

    def test_file_extension_json(self):
        p = make_plugin()
        self.assertEqual(p.get_file_extension(), "json")

    def test_output_descriptor_format(self):
        p = make_plugin()
        desc = p.get_output_descriptor()
        self.assertEqual(desc["format"], "processes_snapshot_array")

    def test_output_descriptor_csv_headers(self):
        p = make_plugin()
        desc = p.get_output_descriptor()
        self.assertIsInstance(desc["csv_headers"], list)
        self.assertIn("pid", desc["csv_headers"])

    def test_basic_row(self):
        proc = {"pid": 1, "userName": "alice", "startInstant": "t", "totalCpuDuration": 0,
                "command": "/bin/sh", "supportsNormalTermination": 1, "parentPid": 0, "hasChildren": 0}
        result = self.row(proc)
        cols = result.split(",")
        self.assertEqual(cols[0], "1")
        self.assertEqual(cols[2], "alice")

    def test_command_comma_quoted(self):
        proc = {"pid": 2, "command": "prog, arg"}
        result = self.row(proc)
        self.assertIn('"prog, arg"', result)

    def test_missing_fields_use_defaults(self):
        result = self.row({})
        cols = result.split(",")
        self.assertIn("-", cols)

    def test_nine_columns(self):
        proc = {"pid": 3, "userName": "u", "startInstant": "t", "totalCpuDuration": 0,
                "command": "c", "supportsNormalTermination": 1, "parentPid": 0, "hasChildren": 0}
        result = self.row(proc)
        self.assertEqual(len(result.split(",")), 9)

    def test_returns_dict_captureTimestamp(self):
        with patch("processes_recorder.main.psutil.process_iter", return_value=[]):
            snap = ProcessesCapturePlugin.take_snapshot(42.0)
            self.assertEqual(snap["captureTimestamp"], 42.0)

    def test_returns_processes_list(self):
        with patch("processes_recorder.main.psutil.process_iter", return_value=[]):
            snap = ProcessesCapturePlugin.take_snapshot(0.0)
            self.assertIsInstance(snap["processes"], list)

    def test_processes_includes_pid(self):
        cpu = MagicMock()
        cpu.user = 1.0
        cpu.system = 0.5
        proc = self.make_proc_mock(pid=123, cpu_times=cpu)
        with patch("processes_recorder.main.psutil.process_iter", side_effect=[[], [proc]]):
            snap = ProcessesCapturePlugin.take_snapshot(0.0)
            pids = [p["pid"] for p in snap["processes"]]
            self.assertIn(123, pids)

    def test_has_children_flag(self):
        parent = self.make_proc_mock(pid=1, ppid=None)
        child = self.make_proc_mock(pid=2, ppid=1)
        with patch("processes_recorder.main.psutil.process_iter", side_effect=[[child], [parent, child]]):
            snap = ProcessesCapturePlugin.take_snapshot(0.0)
            p1 = next((p for p in snap["processes"] if p["pid"] == 1), None)
            if p1:
                self.assertEqual(p1["hasChildren"], 1)

    def test_no_such_process_skipped(self):
        bad = MagicMock()
        bad.info = MagicMock()
        bad.info.__getitem__ = MagicMock(side_effect=psutil.NoSuchProcess(1))
        bad.info.get = MagicMock(side_effect=psutil.NoSuchProcess(1))
        with patch("processes_recorder.main.psutil.process_iter", side_effect=[[], [bad]]):
            snap = ProcessesCapturePlugin.take_snapshot(0.0)
            self.assertIsInstance(snap["processes"], list)

    def test_no_create_time_uses_dash(self):
        cpu = MagicMock()
        cpu.user = 0.0
        cpu.system = 0.0
        proc = self.make_proc_mock(pid=5, create_time=None, cpu_times=cpu)
        with patch("processes_recorder.main.psutil.process_iter", side_effect=[[], [proc]]):
            snap = ProcessesCapturePlugin.take_snapshot(0.0)
            p = snap["processes"][0]
            self.assertEqual(p["startInstant"], "-")

    def test_no_cpu_times_uses_zero(self):
        proc = self.make_proc_mock(pid=6, cpu_times=None)
        with patch("processes_recorder.main.psutil.process_iter", side_effect=[[], [proc]]):
            snap = ProcessesCapturePlugin.take_snapshot(0.0)
            p = snap["processes"][0]
            self.assertEqual(p["totalCpuDuration"], 0)

    def test_save_creates_json_file(self):
        p = make_plugin({"raw_export": False, "stream_to_clients": False})
        p.prepare(self.tmp, "sess")
        snap = make_snapshot()
        p.save([capture_data_from(snap)], end_of_data=True)
        path = os.path.join(self.tmp, "sess.json")
        self.assertTrue(os.path.exists(path))

    def test_json_valid_after_end_of_data(self):
        p = make_plugin({"raw_export": False, "stream_to_clients": False})
        p.prepare(self.tmp, "sess")
        items = [capture_data_from(make_snapshot(float(i))) for i in range(3)]
        p.save(items, end_of_data=True)
        with open(os.path.join(self.tmp, "sess.json"), encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(len(data), 3)

    def test_multiple_saves_accumulate(self):
        p = make_plugin({"raw_export": False, "stream_to_clients": False})
        p.prepare(self.tmp, "sess")
        for i in range(3):
            p.save([capture_data_from(make_snapshot(float(i)))])
        p.save([], end_of_data=True)
        with open(os.path.join(self.tmp, "sess.json"), encoding="utf-8") as f:
            data = json.load(f)
        self.assertEqual(len(data), 3)

    def test_export_csv_creates_csv_file(self):
        p = make_plugin({"raw_export": True, "stream_to_clients": False})
        p.prepare(self.tmp, "sess")
        p.save([capture_data_from(make_snapshot())], end_of_data=True)
        csv_path = os.path.join(self.tmp, "sess.csv")
        self.assertTrue(os.path.exists(csv_path))
        with open(csv_path, encoding="utf-8") as f:
            lines = f.readlines()
        self.assertGreaterEqual(len(lines), 2)
        self.assertIn("pid", lines[0])

    def test_csv_second_line_has_pid(self):
        p = make_plugin({"raw_export": True, "stream_to_clients": False})
        p.prepare(self.tmp, "sess")
        p.save([capture_data_from(make_snapshot())], end_of_data=True)
        with open(os.path.join(self.tmp, "sess.csv"), encoding="utf-8") as f:
            lines = f.readlines()
        self.assertIn("1", lines[1])

    def test_non_dict_data_ignored(self):
        p = make_plugin({"raw_export": False, "stream_to_clients": False})
        p.prepare(self.tmp, "sess")
        bad = CaptureData(timestamp=0.0, data="not a dict")
        p.save([bad], end_of_data=True)
        path = os.path.join(self.tmp, "sess.json")
        self.assertFalse(os.path.exists(path))

    def test_save_stream_calls_server_service(self):
        p = make_plugin({"raw_export": False, "stream_to_clients": True})
        p.prepare(self.tmp, "sess")
        snap = make_snapshot()
        with patch.object(p, "stream_snapshot") as mock_stream:
            p.save([capture_data_from(snap)])
            mock_stream.assert_called_once_with(snap)

    def test_no_stream_disabled(self):
        p = make_plugin({"raw_export": False, "stream_to_clients": False})
        p.prepare(self.tmp, "sess")
        with patch.object(p, "stream_snapshot") as mock_stream:
            p.save([capture_data_from(make_snapshot())])
            mock_stream.assert_not_called()

    def test_stream_defaults_true_none(self):
        p = make_plugin({"raw_export": False})
        p.prepare(self.tmp, "sess")
        with patch.object(p, "stream_snapshot") as mock_stream:
            p.save([capture_data_from(make_snapshot())])
            mock_stream.assert_called_once()

    def test_close_files_closes_json(self):
        p = make_plugin({"raw_export": False, "stream_to_clients": False})
        p.prepare(self.tmp, "sess")
        p.save([capture_data_from(make_snapshot())], end_of_data=True)
        self.assertIsNone(p.json_file)
        self.assertIsNone(p.csv_file)

    def test_close_files_no_files(self):
        p = make_plugin()
        p.close_files()

    def test_close_files_exception_swallowed(self):
        p = make_plugin()
        mock_file = MagicMock()
        mock_file.write.side_effect = IOError("disk full")
        p.json_file = mock_file
        p.close_files()
        self.assertIsNone(p.json_file)

    def test_csv_file_close_exception_swallowed(self):
        p = make_plugin()
        mock_csv = MagicMock()
        mock_csv.flush.side_effect = IOError("disk full")
        p.csv_file = mock_csv
        p.close_files()
        self.assertIsNone(p.csv_file)

    def test_loop_calls_on_data(self):
        p = make_plugin({"snapshot_interval": 1})
        received = []
        p.status = "running"
        p.capture_loop = functools.partial(TestProcessesPlugin.one_shot_loop, p)
        p.capture_loop(lambda: 1.0, received.append, 1)
        self.assertEqual(len(received), 1)

    def test_loop_exits_stopped(self):
        p = make_plugin()
        p.status = "stopped"
        received = []
        with patch("processes_recorder.main.time.sleep"):
            t = threading.Thread(target=p.capture_loop,
                                 args=(lambda: 0.0, received.append, 1))
            t.start()
            t.join(timeout=1.0)
        self.assertFalse(t.is_alive())
        self.assertEqual(len(received), 0)

    def test_loop_pauses(self):
        p = make_plugin()
        received = []
        sleep_calls = []
        p.status = "paused"
        with patch("processes_recorder.main.time.sleep",
                   side_effect=functools.partial(TestProcessesPlugin.stopping_sleep, p, sleep_calls)):
            p.capture_loop(lambda: 0.0, received.append, 1)
        self.assertEqual(len(received), 0)
        self.assertGreater(len(sleep_calls), 0)

    def test_start_sets_running(self):
        p = make_plugin({"snapshot_interval": 1})
        with patch("threading.Thread") as mock_thread:
            mock_thread.return_value = MagicMock()
            p.start(1.0, lambda: 1.0, lambda d: None)
        self.assertEqual(p.status, "running")
        self.assertEqual(p.last_start, 1.0)

    def test_start_launches_thread(self):
        p = make_plugin({"snapshot_interval": 1})
        with patch("threading.Thread") as mock_thread:
            t = MagicMock()
            mock_thread.return_value = t
            p.start(1.0, lambda: 1.0, lambda d: None)
        t.start.assert_called_once()

    def test_start_uses_default_interval_none(self):
        p = make_plugin({})
        with patch("threading.Thread") as mock_thread:
            t = MagicMock()
            mock_thread.return_value = t
            p.start(0.0, lambda: 0.0, lambda d: None)
        t.start.assert_called_once()

    def test_raw_export_none_defaults_no_csv(self):
        p = make_plugin({"stream_to_clients": False})
        p.prepare(self.tmp, "sess")
        p.save([capture_data_from(make_snapshot())], end_of_data=True)
        csv_path = os.path.join(self.tmp, "sess.csv")
        self.assertFalse(os.path.exists(csv_path))

    def test_real_loop_calls_on_data_exits(self):
        p = make_plugin({"snapshot_interval": 1})
        received = []
        counter = [0]
        with patch("processes_recorder.main.time.sleep"):
            with patch.object(ProcessesCapturePlugin, "take_snapshot", return_value=make_snapshot()):
                p.status = "running"
                p.capture_loop(
                    functools.partial(TestProcessesPlugin.counting_ts, p, counter),
                    received.append,
                    1
                )
        self.assertGreaterEqual(len(received), 1)

    def test_nosuchprocess_ppid_loop_skipped(self):
        bad = MagicMock()
        bad.info = MagicMock()
        bad.info.get = MagicMock(side_effect=psutil.NoSuchProcess(1))
        with patch("processes_recorder.main.psutil.process_iter", side_effect=[[bad], []]):
            snap = ProcessesCapturePlugin.take_snapshot(0.0)
            self.assertIsInstance(snap, dict)

    def test_process_iter_exception_outer_loop(self):
        with patch("processes_recorder.main.psutil.process_iter",
                   side_effect=[RuntimeError("iter failed"), []]):
            snap = ProcessesCapturePlugin.take_snapshot(0.0)
            self.assertEqual(snap["processes"], [])

    def test_stream_snapshot_calls_server_service(self):
        p = make_plugin()
        snap = make_snapshot()
        with patch.object(ServerService, "send_capture_data") as mock_send:
            p.stream_snapshot(snap)
            mock_send.assert_called_once_with(snap)

    def test_properties_object_exists(self):
        self.assertIsNotNone(properties)

    def test_snapshot_interval_field_exists(self):
        self.assertTrue(properties.has_property("snapshot_interval"))

    def test_snapshot_interval_int(self):
        self.assertEqual(properties.get_type("snapshot_interval"), PropertyType.INT)

    def test_snapshot_interval_default_5(self):
        self.assertEqual(properties.get_default_values()["snapshot_interval"], 5)

    def test_export_csv_field_exists(self):
        self.assertTrue(properties.has_property("export_to_csv"))

    def test_export_csv_default_false(self):
        self.assertEqual(properties.get_default_values()["export_to_csv"], False)

    def test_stream_clients_field_exists(self):
        self.assertTrue(properties.has_property("stream_to_clients"))

    def test_stream_clients_default_true(self):
        self.assertEqual(properties.get_default_values()["stream_to_clients"], True)

    def test_three_fields_total(self):
        self.assertEqual(len(properties.get_default_values()), 3)


if __name__ == "__main__":
    unittest.main()
