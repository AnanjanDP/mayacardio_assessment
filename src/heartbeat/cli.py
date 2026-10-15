"""
Command Line Interface for Never Lose a Heartbeat.
Implements required 'status', 'export', and 'results' commands, plus 'run' and 'sync'.
Outputs strict JSON lines without corrupting diagnostic text.
"""

import argparse
import json
import logging
import os
import sys
import threading
import time

from heartbeat.cloud_client import CloudClient
from heartbeat.collector import SensorCollector
from heartbeat.config import PipelineConfig
from heartbeat.connectivity import ModeManager
from heartbeat.loss_tracker import LossTracker
from heartbeat.processor import LocalProcessor
from heartbeat.storage import DeviceStorage
from heartbeat.uploader import BacklogUploader


def configure_logging(level_name: str = "WARNING"):
    """Configures logging to stderr so stdout remains clean for JSON Lines."""
    level = getattr(logging, level_name.upper(), logging.WARNING)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )


def cmd_status(args, config: PipelineConfig):
    """
    Prints exactly one JSON line:
    {"mode": "online", "samples_saved": 120400, "samples_in_cloud": 120400, "backlog": 0}
    """
    storage = DeviceStorage(db_path=config.db_path, synchronous=config.sqlite_synchronous)
    cloud_client = CloudClient(base_url=config.cloud_url)
    mode_mgr = ModeManager(cloud_client=cloud_client)

    current_mode = mode_mgr.check_now()
    saved, in_cloud, backlog = storage.get_sync_status()

    # Reconcile with actual cloud reported count if online
    if current_mode == "online":
        cloud_stat = cloud_client.get_cloud_status()
        if cloud_stat and "total_samples" in cloud_stat:
            in_cloud = cloud_stat["total_samples"]
            backlog = max(0, saved - in_cloud)

    status_obj = {
        "mode": current_mode,
        "samples_saved": saved,
        "samples_in_cloud": in_cloud,
        "backlog": backlog,
        "session_id": storage.get_latest_session_id(),
    }
    sys.stdout.write(json.dumps(status_obj) + "\n")
    sys.stdout.flush()


def cmd_sessions(args, config: PipelineConfig):
    """
    Lists all recorded sessions with metadata and sample counts.
    """
    storage = DeviceStorage(db_path=config.db_path)
    sessions = storage.list_sessions()
    if not sessions:
        sys.stderr.write("No sessions found in database.\n")
        return
    for s in sessions:
        sys.stdout.write(json.dumps(s) + "\n")
    sys.stdout.flush()


def cmd_export(args, config: PipelineConfig):
    """
    Exports samples and loss records as JSON lines from device or cloud.
    """
    storage = DeviceStorage(db_path=config.db_path)
    session_id = args.session
    if session_id.lower() in ("latest", "last", "current"):
        session_id = storage.get_latest_session_id()
        if not session_id:
            sys.stderr.write("No session found in database.\n")
            sys.exit(1)

    from_source = getattr(args, "from")

    if from_source == "device":
        for line in storage.export_session_samples(session_id):
            sys.stdout.write(line + "\n")
        for line in storage.export_session_losses(session_id):
            sys.stdout.write(line + "\n")
        sys.stdout.flush()
    elif from_source == "cloud":
        cloud_client = CloudClient(base_url=config.cloud_url)
        try:
            for line in cloud_client.export_cloud_data(session_id):
                sys.stdout.write(line + "\n")
            sys.stdout.flush()
        except Exception as e:
            sys.stderr.write(f"Error exporting from cloud: {e}\n")
            sys.exit(1)
    else:
        sys.stderr.write(f"Invalid --from source: {from_source}\n")
        sys.exit(1)


def cmd_results(args, config: PipelineConfig):
    """
    Prints processing results for the given session.
    """
    storage = DeviceStorage(db_path=config.db_path)
    session_id = args.session
    if session_id.lower() in ("latest", "last", "current"):
        session_id = storage.get_latest_session_id()
        if not session_id:
            sys.stderr.write("No session found in database.\n")
            sys.exit(1)

    cloud_client = CloudClient(base_url=config.cloud_url)

    # Check if results are in cloud first
    has_cloud_results = False
    try:
        results = list(cloud_client.export_cloud_results(session_id))
        if results:
            has_cloud_results = True
            for line in results:
                sys.stdout.write(line + "\n")
            sys.stdout.flush()
    except Exception:
        pass

    if not has_cloud_results:
        # Fallback to device storage offline results (compute on demand if needed)
        results_list = list(storage.export_results(session_id))
        if not results_list:
            processor = LocalProcessor(storage)
            processor.process_pending_windows(session_id, mode="offline")
            results_list = list(storage.export_results(session_id))

        for line in results_list:
            sys.stdout.write(line + "\n")
        sys.stdout.flush()


def cmd_run(args, config: PipelineConfig):
    """
    Runs the full edge pipeline: collector, mode manager, uploader, local processor.
    """
    configure_logging(config.log_level)
    logging.info("Starting Never Lose a Heartbeat edge pipeline...")

    storage = DeviceStorage(db_path=config.db_path, synchronous=config.sqlite_synchronous)
    loss_tracker = LossTracker(storage)
    cloud_client = CloudClient(base_url=config.cloud_url)

    stop_event = threading.Event()

    collector = SensorCollector(config, storage, loss_tracker, stop_event)
    mode_mgr = ModeManager(cloud_client, stop_event=stop_event)
    uploader = BacklogUploader(storage, cloud_client, mode_mgr, batch_size=config.sync_batch_size, stop_event=stop_event)
    local_processor = LocalProcessor(storage)

    # Background threads
    t_mode = threading.Thread(target=mode_mgr.run_loop, daemon=True, name="ModeManager")
    t_upload = threading.Thread(target=uploader.run_loop, daemon=True, name="Uploader")

    t_mode.start()
    t_upload.start()

    def offline_processor_loop():
        while not stop_event.is_set():
            if mode_mgr.get_mode() == "offline" and collector.current_session_id:
                local_processor.process_pending_windows(collector.current_session_id, mode="offline")
            time.sleep(1.0)

    t_proc = threading.Thread(target=offline_processor_loop, daemon=True, name="LocalProcessor")
    t_proc.start()

    try:
        collector.run_loop()
    except KeyboardInterrupt:
        logging.info("Shutdown requested via SIGINT...")
    finally:
        stop_event.set()
        collector.close()
        logging.info("Pipeline stopped cleanly.")


def cmd_sync(args, config: PipelineConfig):
    """Triggers an immediate catchup synchronization."""
    storage = DeviceStorage(db_path=config.db_path)
    cloud_client = CloudClient(base_url=config.cloud_url)
    mode_mgr = ModeManager(cloud_client)

    if mode_mgr.check_now() != "online":
        sys.stderr.write("Cannot sync: cloud is currently offline\n")
        sys.exit(1)

    uploader = BacklogUploader(storage, cloud_client, mode_mgr, batch_size=config.sync_batch_size)
    synced = uploader.sync_all_sessions(drain_backlog=True)
    sys.stderr.write(f"Synced {synced} samples to cloud.\n")


def main():
    config = PipelineConfig()

    parser = argparse.ArgumentParser(prog="pipeline", description="Never Lose a Heartbeat CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # status
    subparsers.add_parser("status", help="Print pipeline status JSON")

    # sessions
    subparsers.add_parser("sessions", help="List all recorded sessions and sample counts")

    # export
    parser_export = subparsers.add_parser("export", help="Export samples and loss records")
    parser_export.add_argument("--session", default="latest", help="Session ID to export (or 'latest')")
    parser_export.add_argument("--from", dest="from", choices=["device", "cloud"], required=True, help="Source to export from")

    # results
    parser_results = subparsers.add_parser("results", help="Print processing results")
    parser_results.add_argument("--session", default="latest", help="Session ID (or 'latest')")

    # run
    subparsers.add_parser("run", help="Run edge collector and synchronizer")

    # sync
    subparsers.add_parser("sync", help="Trigger immediate backlog synchronization")

    args = parser.parse_args()

    if args.command == "status":
        cmd_status(args, config)
    elif args.command == "sessions":
        cmd_sessions(args, config)
    elif args.command == "export":
        cmd_export(args, config)
    elif args.command == "results":
        cmd_results(args, config)
    elif args.command == "run":
        cmd_run(args, config)
    elif args.command == "sync":
        cmd_sync(args, config)


if __name__ == "__main__":
    main()
