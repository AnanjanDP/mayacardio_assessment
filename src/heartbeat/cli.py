import argparse
import itertools
import json
import logging
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
    level = getattr(logging, level_name.upper(), logging.WARNING)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )


def _emit(lines) -> None:
    for line in lines:
        sys.stdout.write(line + "\n")
    sys.stdout.flush()


def _resolve_session(storage, session_id: str) -> str:
    if session_id.lower() in ("latest", "last", "current"):
        session_id = storage.get_latest_session_id()
        if not session_id:
            sys.stderr.write("No session found in database.\n")
            sys.exit(1)
    return session_id


def cmd_status(args, config: PipelineConfig):
    storage = DeviceStorage(db_path=config.db_path, synchronous=config.sqlite_synchronous)
    cloud_client = CloudClient(base_url=config.cloud_url)
    mode_mgr = ModeManager(cloud_client=cloud_client)

    current_mode = mode_mgr.check_now()
    saved, in_cloud, backlog = storage.get_sync_status()

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
    _emit([json.dumps(status_obj)])


def cmd_sessions(args, config: PipelineConfig):
    storage = DeviceStorage(db_path=config.db_path)
    sessions = storage.list_sessions()
    if not sessions:
        sys.stderr.write("No sessions found in database.\n")
        return
    _emit(json.dumps(s) for s in sessions)


def cmd_export(args, config: PipelineConfig):
    storage = DeviceStorage(db_path=config.db_path)
    session_id = _resolve_session(storage, args.session)

    from_source = getattr(args, "from")

    if from_source == "device":
        _emit(itertools.chain(storage.export_session_samples(session_id), storage.export_session_losses(session_id)))
    elif from_source == "cloud":
        cloud_client = CloudClient(base_url=config.cloud_url)
        try:
            for line in cloud_client.export_cloud_data(session_id):
                sys.stdout.write(line + "\n")
            sys.stdout.flush()
        except Exception as e:
            sys.stderr.write(f"Error exporting from cloud: {e}\n")
            sys.exit(1)


def cmd_results(args, config: PipelineConfig):
    storage = DeviceStorage(db_path=config.db_path)
    session_id = _resolve_session(storage, args.session)

    cloud_client = CloudClient(base_url=config.cloud_url)

    has_cloud_results = False
    try:
        results = list(cloud_client.export_cloud_results(session_id))
        if results:
            has_cloud_results = True
            _emit(results)
    except Exception:
        logging.getLogger(__name__).debug("cloud results unavailable", exc_info=True)

    if not has_cloud_results:
        results_list = list(storage.export_results(session_id))
        if not results_list:
            processor = LocalProcessor(storage)
            processor.process_pending_windows(session_id, mode="offline")
            results_list = list(storage.export_results(session_id))

        _emit(results_list)


def cmd_run(args, config: PipelineConfig):
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

    subparsers.add_parser("status", help="Print pipeline status JSON")

    subparsers.add_parser("sessions", help="List all recorded sessions and sample counts")

    parser_export = subparsers.add_parser("export", help="Export samples and loss records")
    parser_export.add_argument("--session", default="latest", help="Session ID to export (or 'latest')")
    parser_export.add_argument("--from", dest="from", choices=["device", "cloud"], required=True, help="Source to export from")

    parser_results = subparsers.add_parser("results", help="Print processing results")
    parser_results.add_argument("--session", default="latest", help="Session ID (or 'latest')")

    subparsers.add_parser("run", help="Run edge collector and synchronizer")

    subparsers.add_parser("sync", help="Trigger immediate backlog synchronization")

    args = parser.parse_args()

    handlers = {
        "status": cmd_status,
        "sessions": cmd_sessions,
        "export": cmd_export,
        "results": cmd_results,
        "run": cmd_run,
        "sync": cmd_sync,
    }
    handlers[args.command](args, config)


if __name__ == "__main__":
    main()
