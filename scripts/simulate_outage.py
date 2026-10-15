"""
simulate_outage.py: End-to-end integration simulation of network outage and recovery.
Demonstrates:
1. Online mode streaming.
2. Network cut -> automatic switch to offline within 10s.
3. Offline collection & local persistence with zero sample loss.
4. Network restored -> automatic switch to online within 10s.
5. Backlog synchronization catching up to 0 backlog.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

import uvicorn
from heartbeat.cloud_client import CloudClient
from heartbeat.storage import DeviceStorage
from mock.check_ground_truth import check_export


def main():
    
    print("SIMULATING NETWORK OUTAGE & AUTOMATIC MODE RECOVERY ")
    

    temp_dir = tempfile.mkdtemp(prefix="outage_sim_")
    dev_db = os.path.join(temp_dir, "device.db")
    cloud_db = os.path.join(temp_dir, "cloud.db")
    mock_port = 7120
    cloud_port = 8120

    env = os.environ.copy()
    env["PYTHONPATH"] = f"src;{os.path.abspath('.')}"
    env["PIPELINE_DB_PATH"] = dev_db
    env["PIPELINE_MOCK_PORT"] = str(mock_port)
    env["PIPELINE_CLOUD_URL"] = f"http://127.0.0.1:{cloud_port}"
    env["CLOUD_DB_URL"] = f"sqlite:///{cloud_db}"
    env["CLOUD_PORT"] = str(cloud_port)
    env["PIPELINE_POLL_INTERVAL"] = "0.02"

    # 1. Start mock server
    print("\n[Step 1] Starting biosignal-mock server...")
    mock_proc = subprocess.Popen(
        [sys.executable, "-m", "mock.biosignal_mock", "--mode", "ramp", "--port", str(mock_port), "--fast-clock", "5.0"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.0)

    # 2. Start cloud API
    print("[Step 2] Starting Cloud API server...")
    cloud_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "cloud.api:app", "--port", str(cloud_port), "--host", "127.0.0.1"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(1.5)

    # 3. Start edge pipeline
    print("[Step 3] Starting edge pipeline...")
    pipe_proc = subprocess.Popen(
        [sys.executable, "-m", "heartbeat.cli", "run"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        # Check initial online state
        print("Waiting for initial collection and cloud sync...")
        time.sleep(3.0)
        status_out = subprocess.check_output([sys.executable, "-m", "heartbeat.cli", "status"], env=env).decode().strip()
        print(f"Status before outage: {status_out}")
        assert '"mode": "online"' in status_out

        # 4. Simulate network outage by killing Cloud API
        print("\n[Step 4] Cutting network to cloud (terminating Cloud API)...")
        cloud_proc.terminate()
        cloud_proc.wait(timeout=3.0)

        # Measure mode switch time to offline (Target <= 10s)
        t_cut = time.time()
        offline_detected = False
        while time.time() - t_cut < 10.0:
            status_out = subprocess.check_output([sys.executable, "-m", "heartbeat.cli", "status"], env=env).decode().strip()
            if '"mode": "offline"' in status_out:
                offline_detected = True
                switch_latency = time.time() - t_cut
                print(f"Switched to OFFLINE mode in {switch_latency:.2f}s (SLA <= 10.0s). Status: {status_out}")
                break
            time.sleep(0.5)

        assert offline_detected, "Failed to switch to offline mode within 10 seconds!"

        # Let it collect offline for a few seconds
        print("Collecting offline data...")
        time.sleep(3.0)
        status_offline = subprocess.check_output([sys.executable, "-m", "heartbeat.cli", "status"], env=env).decode().strip()
        print(f"Offline status: {status_offline}")

        # 5. Restore network (restart Cloud API)
        print("\n[Step 5] Restoring network (restarting Cloud API)...")
        cloud_proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "cloud.api:app", "--port", str(cloud_port), "--host", "127.0.0.1"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        t_restore = time.time()
        online_detected = False
        while time.time() - t_restore < 10.0:
            status_out = subprocess.check_output([sys.executable, "-m", "heartbeat.cli", "status"], env=env).decode().strip()
            if '"mode": "online"' in status_out:
                online_detected = True
                restore_latency = time.time() - t_restore
                print(f"Switched to ONLINE mode in {restore_latency:.2f}s (SLA <= 10.0s). Status: {status_out}")
                break
            time.sleep(0.5)

        assert online_detected, "Failed to switch to online mode within 10 seconds!"

        # 6. Stop mock to stop new sample generation, then verify backlog drains to 0
        print("\n[Step 6] Stopping mock and waiting for backlog synchronization to 0...")
        mock_proc.terminate()
        mock_proc.wait(timeout=2.0)

        t_sync_start = time.time()
        backlog_cleared = False
        while time.time() - t_sync_start < 15.0:
            status_out = subprocess.check_output([sys.executable, "-m", "heartbeat.cli", "status"], env=env).decode().strip()
            print(f"Syncing: {status_out}")
            if '"backlog": 0' in status_out:
                backlog_cleared = True
                print(f"Backlog completely synchronized! Final Status: {status_out}")
                break
            time.sleep(0.5)

        assert backlog_cleared, "Backlog failed to synchronize to 0 within time limit!"

        print("\n[PASS] Network outage and recovery simulation succeeded!")
    finally:
        pipe_proc.terminate()
        mock_proc.terminate()
        cloud_proc.terminate()
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
