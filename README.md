# Processes Capture Plugin

A plugin for [**Multimodal Observer**](https://github.com/MultimodalObserver-2/mo) that records periodic snapshots of all running system processes, saving timestamped data to a JSON file and optionally to a CSV file.

## Features

- Captures all running processes at a configurable interval
- Records PID, user, start time, CPU duration, executable path, parent PID, and child relationships
- Optional CSV export for spreadsheet-compatible analysis
- Optional real-time streaming of snapshots to connected clients via the Multimodal Observer server
- Supports pause and resume during a recording session
- Saves data in `.json` format

## Configuration Options

| Property | Description | Default |
| -------- | ----------- | ------- |
| `snapshot_interval` | Interval in seconds between process snapshots | `5` |
| `export_to_csv` | Also export data to a `.csv` file | `false` |
| `stream_to_clients` | Stream snapshots to connected clients in real time | `true` |

These can be set in the plugin configuration interface of Multimodal Observer.

## Output Format

The plugin outputs a JSON array where each entry is a snapshot. Example:

```json
{
  "captureTimestamp": 1.042,
  "processes": [
    {
      "pid": 1234,
      "userName": "alice",
      "startInstant": "2024-01-01T10:00:00+00:00",
      "totalCpuDuration": 1500,
      "command": "/usr/bin/python3",
      "parentPid": 1,
      "hasChildren": 0,
      "supportsNormalTermination": 1
    }
  ]
}
```

## Installation

### 1. Build the plugin

```
build-mop -r requirements.txt
```

This generates the distributable `.zip` file inside the `dist/` folder.

### 2. Register the plugin

Open Multimodal Observer, go to the plugin interface, and register the `.zip` file located in the `dist/` folder.

## How It Works

- Spawns a background thread that calls `psutil.process_iter()` at the configured interval.
- Each snapshot includes all running processes with their CPU usage, hierarchy, and metadata.
- Snapshots are forwarded to Multimodal Observer as `CaptureData` objects and optionally written to CSV.
- Pause and resume are handled by skipping snapshot collection while paused.
