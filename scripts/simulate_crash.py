import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from heartbeat.storage import DeviceStorage
from mock.check_ground_truth import check_export


def main():
    print("=================================================================")
    print("  SIMULATING PROCESS CRASH & DURABLE RECOVERY                    ")
    print("=================================================================")

    temp_dir = tempfile.mkdtemp(prefix="crash_sim_")
    dev_db = os.path.join(temp_dir, "crash_device.db")
    mock_port = 7125

    env = os.environ.copy()
    env["PYTHONPATH"] = f"src;{os.path.abspath('.')}"
    env["PIPELINE_DB_PATH"] = dev_db
    env["PIPELINE_MOCK_PORT"] = str(mock_port)
    env["PIPELINE_CLOUD_URL"] = "http://127.0.0.1:9999"
    env["PIPELINE_POLL_INTERVAL"] = "0.02"

    print("\n[Step 1] Starting biosignal-mock server (mode=ramp)...")
    mock_proc = subprocess.Popen(
        [sys.executable, "-m", "mock.biosignal_mock", "--mode", "ramp", "--port", str(mock_port), "--fast-clock", "5.0"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.0)

    print("[Step 2] Starting edge pipeline...")
    pipe_proc = subprocess.Popen(
        [sys.executable, "-m", "heartbeat.cli", "run"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        print("Collecting samples...")
        time.sleep(3.0)
        status_before = subprocess.check_output([sys.executable, "-m", "heartbeat.cli", "status"], env=env).decode().strip()
        print(f"Status before crash: {status_before}")

        print("\n[Step 3] Killing edge pipeline process abruptly (SIGKILL / proc.kill)...")
        pipe_proc.kill()
        pipe_proc.wait()
        print("Pipeline process terminated.")

        print("\n[Step 4] Checking SQLite database integrity...")
        conn = sqlite3.connect(dev_db)
        cur = conn.cursor()
        cur.execute("PRAGMA integrity_check;")
        res = cur.fetchone()[0]
        conn.close()
        print(f"PRAGMA integrity_check result: {res}")
        assert res == "ok", f"Integrity check failed: {res}"

        print("\n[Step 5] Restarting edge pipeline...")
        pipe_proc2 = subprocess.Popen(
            [sys.executable, "-m", "heartbeat.cli", "run"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        time.sleep(3.0)
        status_after = subprocess.check_output([sys.executable, "-m", "heartbeat.cli", "status"], env=env).decode().strip()
        print(f"Status after restart: {status_after}")
        pipe_proc2.kill()
        pipe_proc2.wait()

        print("\n[Step 6] Exporting data and verifying with check_ground_truth...")
        storage = DeviceStorage(db_path=dev_db)
        conn = storage.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT session_id FROM sessions LIMIT 1")
        session_id = cur.fetchone()[0]
        conn.close()

        export_path = os.path.join(temp_dir, "crash_export.ndjson")
        with open(export_path, "w", encoding="utf-8") as f:
            for line in storage.export_session_samples(session_id):
                f.write(line + "\n")
            for line in storage.export_session_losses(session_id):
                f.write(line + "\n")

        passed, summary = check_export(export_path, mode="ramp")
        print("Ground Truth Check Summary:")
        import json
        print(json.dumps(summary, indent=2))
        assert passed is True, "Ground truth check failed after crash recovery!"

        print("\n[PASS] Process crash and durable recovery simulation succeeded!")
    finally:
        mock_proc.terminate()
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
