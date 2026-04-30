import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

import psutil

from mo.core import load_metadata_json
from mo.modules.capture import CaptureData, CapturePlugin
from mo.modules.communication.services.server_service import ServerService

CSV_HEADERS = (
    "pid,captureTime,userName,startInstant,"
    "totalCpuDuration,command,supportsNormalTermination,parentPid,hasChildren"
)


def format_csv_field(v: object) -> str:
    s = str(v) if v is not None else ""
    if "," in s or '"' in s or "\n" in s:
        s = '"' + s.replace('"', '""') + '"'
    return s


@load_metadata_json("../..")
class ProcessesCapturePlugin(CapturePlugin):

    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"

    def load(self) -> None:
        self.status = self.STOPPED
        self.capture_thread: threading.Thread | None = None
        self.json_file = None
        self.csv_file = None
        self.json_first = True
        self.output_path: str | None = None
        self.csv_path: str | None = None
        self.last_start = 0.0
        self.last_pause = 0.0
        self.last_resume = 0.0
        self.last_stop = 0.0

    def unload(self) -> None:
        self.status = self.STOPPED
        self.close_files()
        

    def prepare(self, path: str, file_name: str) -> None:
        self.output_path = os.path.join(path, file_name + ".json")
        self.csv_path = os.path.join(path, file_name + ".csv")
        self.json_first = True
        self.json_file = None
        self.csv_file = None
        self.last_start = 0.0
        self.last_pause = 0.0
        self.last_resume = 0.0
        self.last_stop = 0.0

    def start(self, start_ts: float, get_timestamp: Callable[[], float], on_data: Callable[[CaptureData], None], ) -> None:
        self.last_start = start_ts
        interval = int(self.settings.get_setting("snapshot_interval") or 5)
        self.status = self.RUNNING
        self.capture_thread = threading.Thread( target=self.capture_loop, args=(get_timestamp, on_data, interval), name="processes-capture-loop", daemon=True, )
        self.capture_thread.start()

    def pause(self, pause_ts: float) -> None:
        self.last_pause = pause_ts
        self.status = self.PAUSED

    def resume(self, resume_ts: float) -> None:
        self.last_resume = resume_ts
        self.status = self.RUNNING

    def stop(self, stop_ts: float) -> None:
        self.last_stop = stop_ts
        self.status = self.STOPPED

    def save(self, data: list[CaptureData], end_of_data: bool = False) -> None:
        raw_export = self.settings.get_setting("raw_export")
        if raw_export is not None:
            export_csv = bool(raw_export)
        else:
            export_csv = False

        raw_stream = self.settings.get_setting("stream_to_clients")
        if raw_stream is not None:
            stream = bool(raw_stream)
        else:
            stream = True

        for capture_data in data:
            snapshot = capture_data.data
            if not isinstance(snapshot, dict):
                continue

            if self.json_file is None and self.output_path:
                self.json_file = open(self.output_path, "w", encoding="utf-8")
                self.json_file.write("[")
                self.json_first = True

            if export_csv and self.csv_file is None and self.csv_path:
                self.csv_file = open(self.csv_path, "w", encoding="utf-8", newline="")
                self.csv_file.write(CSV_HEADERS + "\n")

            if self.json_file:
                if not self.json_first:
                    self.json_file.write(",\n")
                self.json_file.write(json.dumps(snapshot, ensure_ascii=False, indent=2))
                self.json_first = False

            if export_csv and self.csv_file:
                capture_ts = snapshot.get("captureTimestamp", 0)
                for proc in snapshot.get("processes", []):
                    row = self.proc_to_csv_row(proc, capture_ts)
                    self.csv_file.write(row + "\n")

            if stream:
                self.stream_snapshot(snapshot)

        if end_of_data:
            self.close_files()

    def get_file_extension(self) -> str:
        return "json"

    def get_output_descriptor(self) -> dict[str, Any] | None:
        return {
            "format": "processes_snapshot_array",
            "csv_headers": CSV_HEADERS.split(","),
        }

    def capture_loop(self, get_timestamp: Callable[[], float], on_data: Callable[[CaptureData], None], interval: int, ) -> None:
        while self.status != self.STOPPED:
            if self.status == self.RUNNING:
                ts = get_timestamp()
                snapshot = self.take_snapshot(ts)
                on_data(CaptureData(timestamp=ts, data=snapshot))
                elapsed = 0.0
                while elapsed < interval and self.status == self.RUNNING:
                    time.sleep(0.2)
                    elapsed += 0.2
            elif self.status == self.PAUSED:
                time.sleep(0.5)

    @staticmethod
    def take_snapshot(timestamp: float) -> dict:
        parent_pids: set[int] = set()
        try:
            for p in psutil.process_iter(["ppid"]):
                try:
                    ppid = p.info.get("ppid")
                    if ppid:
                        parent_pids.add(ppid)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except Exception:
            pass

        processes: list[dict] = []
        for p in psutil.process_iter(
            ["pid", "username", "create_time", "cpu_times", "exe", "ppid"]
        ):
            try:
                info = p.info
                pid = info["pid"]
                cpu_times = info.get("cpu_times")
                cpu_ms = (
                    int((cpu_times.user + cpu_times.system) * 1000)
                    if cpu_times
                    else 0
                )

                create_time = info.get("create_time")
                if create_time:
                    start_instant = datetime.fromtimestamp(create_time, tz=timezone.utc).isoformat()
                else:                
                    start_instant = "-"

                processes.append(
                    {
                        "pid": pid,
                        "userName": info.get("username") or "-",
                        "startInstant": start_instant,
                        "totalCpuDuration": cpu_ms,
                        "command": info.get("exe") or "-",
                        "parentPid": info.get("ppid") or -1,
                        "hasChildren": 1 if pid in parent_pids else 0,
                        "supportsNormalTermination": 1,
                    }
                )
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

        return {"captureTimestamp": timestamp, "processes": processes}

    def stream_snapshot(self, snapshot: dict) -> None:
        ServerService().send_capture_data(snapshot)


    def close_files(self) -> None:
        if self.json_file:
            try:
                self.json_file.write("]")
                self.json_file.flush()
                self.json_file.close()
            except Exception:
                pass
            self.json_file = None

        if self.csv_file:
            try:
                self.csv_file.flush()
                self.csv_file.close()
            except Exception:
                pass
            self.csv_file = None

    @staticmethod
    def proc_to_csv_row(proc: dict, capture_ts: float) -> str:
        return ",".join(
            [
                format_csv_field(proc.get("pid", "")),
                format_csv_field(capture_ts),
                format_csv_field(proc.get("userName", "-")),
                format_csv_field(proc.get("startInstant", "-")),
                format_csv_field(proc.get("totalCpuDuration", 0)),
                format_csv_field(proc.get("command", "-")),
                format_csv_field(proc.get("supportsNormalTermination", 1)),
                format_csv_field(proc.get("parentPid", -1)),
                format_csv_field(proc.get("hasChildren", 0)),
            ]
        )
